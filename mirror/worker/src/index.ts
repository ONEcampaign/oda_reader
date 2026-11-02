const PREFIX = "oecd/";
const MANIFEST_KEY = "oecd/manifest.json";
const RETRY_AFTER_SECONDS = "60";

type Env = {
  BUCKET: R2Bucket;
  FILES: RateLimit;
  OTHER: RateLimit;
};

function text(
  status: number,
  body: string,
  headers: Record<string, string> = {},
): Response {
  return new Response(`${body}\n`, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
      ...headers,
    },
  });
}

/** Bucket key for a request path, or null when the path is outside `/oecd/`. */
function bucketKey(pathname: string): string | null {
  let key: string;
  try {
    key = decodeURIComponent(pathname.slice(1));
  } catch {
    return null;
  }
  return key.startsWith(PREFIX) ? key : null;
}

/** True when a comma-separated entity-tag header is `*` or lists the tag (weak comparison). */
function listsEtag(header: string, httpEtag: string): boolean {
  const opaque = (tag: string) => tag.trim().replace(/^W\//, "");
  return (
    header.trim() === "*" ||
    header.split(",").some((tag) => opaque(tag) === opaque(httpEtag))
  );
}

/** Status for a failed request precondition, in RFC 9110 order, or null when all pass. */
function failedPrecondition(
  headers: Headers,
  object: R2Object,
): 304 | 412 | null {
  // HTTP dates have whole-second resolution. An absent or invalid date is NaN and never compares true.
  const uploaded = Math.floor(object.uploaded.getTime() / 1000) * 1000;
  const date = (name: string) => Date.parse(headers.get(name) ?? "");

  const ifMatch = headers.get("if-match");
  if (
    ifMatch !== null
      ? !listsEtag(ifMatch, object.httpEtag)
      : uploaded > date("if-unmodified-since")
  ) {
    return 412;
  }
  const ifNoneMatch = headers.get("if-none-match");
  if (
    ifNoneMatch !== null
      ? listsEtag(ifNoneMatch, object.httpEtag)
      : uploaded <= date("if-modified-since")
  ) {
    return 304;
  }
  return null;
}

type ByteRange = { offset: number; length: number };

/**
 * The single byte range a `Range` header asks for, "unsatisfiable" when it lies outside the
 * object, or null when the header is absent, malformed or multi-range (served as a full 200).
 */
function parseRange(
  header: string | null,
  size: number,
): ByteRange | "unsatisfiable" | null {
  const match = /^bytes=(\d*)-(\d*)$/.exec(header ?? "");
  if (!match) return null;
  const [, first = "", last = ""] = match;
  if (first === "" && last === "") return null;

  if (first === "") {
    const suffix = Number(last);
    if (suffix === 0 || size === 0) return "unsatisfiable";
    const length = Math.min(suffix, size);
    return { offset: size - length, length };
  }
  const offset = Number(first);
  const end = last === "" ? size - 1 : Math.min(Number(last), size - 1);
  if (last !== "" && Number(last) < offset) return null;
  if (offset >= size) return "unsatisfiable";
  return { offset, length: end - offset + 1 };
}

export default {
  async fetch(request, env): Promise<Response> {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return text(405, "Method not allowed", { allow: "GET, HEAD" });
    }

    const { pathname } = new URL(request.url);
    const key = bucketKey(pathname);

    // Count every request, including misses, so path scanning is limited too.
    const limiter =
      key !== null && key !== MANIFEST_KEY ? env.FILES : env.OTHER;
    const client = request.headers.get("CF-Connecting-IP") ?? "unknown";
    const { success } = await limiter.limit({ key: client });
    if (!success) {
      return text(429, "Rate limit exceeded. Retry in 60 seconds.", {
        "retry-after": RETRY_AFTER_SECONDS,
      });
    }

    if (key === null) return text(404, "Not found");

    const object = await env.BUCKET.head(key);
    if (object === null) return text(404, "Not found");

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("etag", object.httpEtag);
    headers.set("accept-ranges", "bytes");

    const status = failedPrecondition(request.headers, object);
    if (status !== null) return new Response(null, { status, headers });

    const range = parseRange(request.headers.get("range"), object.size);
    if (range === "unsatisfiable") {
      return text(416, "Range not satisfiable", {
        "content-range": `bytes */${object.size}`,
      });
    }
    headers.set("content-length", String(range?.length ?? object.size));
    if (range) {
      headers.set(
        "content-range",
        `bytes ${range.offset}-${range.offset + range.length - 1}/${object.size}`,
      );
    }

    let body: ReadableStream | null = null;
    if (request.method === "GET") {
      // Reading only the object `head` described keeps the headers and the body consistent.
      const found = await env.BUCKET.get(key, {
        range: range ?? undefined,
        onlyIf: { etagMatches: object.etag },
      });
      if (found === null) return text(404, "Not found");
      if (!("body" in found)) {
        return text(503, "Object changed during the request. Retry.", {
          "retry-after": "1",
        });
      }
      body = found.body;
    }
    return new Response(body, { status: range ? 206 : 200, headers });
  },
} satisfies ExportedHandler<Env>;
