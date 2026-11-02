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

/** Start and length of the served byte range, or null for a full-body response. */
function servedRange(
  object: R2Object,
  requested: boolean,
): { offset: number; length: number } | null {
  if (!requested || !object.range) return null;
  const range = object.range;
  // The runtime resolves ranges to offset and length but may leave an undefined `suffix` key.
  if ("suffix" in range && range.suffix !== undefined) {
    const length = Math.min(range.suffix, object.size);
    return { offset: object.size - length, length };
  }
  const offset = "offset" in range ? (range.offset ?? 0) : 0;
  const length =
    "length" in range
      ? (range.length ?? object.size - offset)
      : object.size - offset;
  return { offset, length: Math.min(length, object.size - offset) };
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

    const wantsRange = request.headers.has("Range");
    const object =
      request.method === "HEAD"
        ? await env.BUCKET.head(key)
        : await env.BUCKET.get(key, {
            range: request.headers,
            onlyIf: request.headers,
          });
    if (object === null) return text(404, "Not found");

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("etag", object.httpEtag);
    headers.set("accept-ranges", "bytes");

    const hasBody = request.method === "GET" && "body" in object;
    // A GET that returns no body failed a conditional header (If-None-Match, If-Modified-Since).
    if (request.method === "GET" && !hasBody)
      return new Response(null, { status: 304, headers });

    const range = servedRange(object, wantsRange);
    const length = range?.length ?? object.size;
    headers.set("content-length", String(length));
    if (range) {
      headers.set(
        "content-range",
        `bytes ${range.offset}-${range.offset + range.length - 1}/${object.size}`,
      );
    }

    const body = hasBody ? (object as R2ObjectBody).body : null;
    return new Response(body, { status: range ? 206 : 200, headers });
  },
} satisfies ExportedHandler<Env>;
