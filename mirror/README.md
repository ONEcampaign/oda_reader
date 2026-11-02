# Bulk-file mirror

The mirror serves byte-identical copies of OECD's bulk files at `https://bulk.developmentfinance.org/oecd/`. `oda_reader` reads it when OECD's file server challenges a download. This directory holds the two parts a maintainer runs:

- `refresher/` and `scripts/mirror/refresh_oecd_mirror.py` copy OECD's files into the R2 bucket `developmentfinance-bulk` and write `oecd/manifest.json`. They run on a maintainer's Mac, because OECD challenges GitHub Actions, Cloudflare Workers, and Cloud Run.
- `worker/` is the Cloudflare Worker that serves the bucket on the custom domain and rate limits each client IP.

## R2 credentials

Create an R2 API token in the ONE Campaign Cloudflare account with the permission Object Read & Write, limited to the bucket `developmentfinance-bulk`. Store its two values in the macOS Keychain. Each command prompts for the value:

```bash
security add-generic-password -s oda-mirror-r2 -a access-key-id -w
security add-generic-password -s oda-mirror-r2 -a secret-access-key -w
```

`refresher/run-refresh.sh` reads these two items. The account id `8b994c58b2ba94f032bbe3bc3fd97d52` is set in that script.

## Run the refresher by hand

From the repository root:

```bash
export R2_ACCOUNT_ID=8b994c58b2ba94f032bbe3bc3fd97d52
export R2_ACCESS_KEY_ID=$(security find-generic-password -s oda-mirror-r2 -a access-key-id -w)
export R2_SECRET_ACCESS_KEY=$(security find-generic-password -s oda-mirror-r2 -a secret-access-key -w)
uv run --group mirror python -m scripts.mirror.refresh_oecd_mirror [--dry-run] [--dataset NAME]
```

`--dry-run` prints what would change and writes nothing. `--dataset` limits the run to one dataset and can repeat. The names are `crs`, `crs-reduced`, `dac1`, `dac2a`, `dac2b`, and `multisystem`. `R2_BUCKET` overrides the bucket name.

A run probes each file's version token and downloads only files whose token changed. It exits 1 if any dataset failed, after refreshing the others. A failed dataset keeps its previous manifest entry.

One run at a time holds the lease `locks/oecd-refresh.json` in the bucket, which records the run's owner id, host, start time, and expiry six hours later. A second run that finds a live lease exits 1 with the holder and expiry in its message and writes nothing. A run publishes the manifest only if no other process changed it since the run read it, and otherwise exits 1 without publishing or deleting anything. A run that crashes leaves its lease live until it expires. If you are sure no refresh is running, clear it from `mirror/worker` with `npx wrangler r2 object delete developmentfinance-bulk/locks/oecd-refresh.json --remote`.

To run the same wrapper the daily job uses, which reads the Keychain and writes the log:

```bash
mirror/refresher/run-refresh.sh [--dry-run]
```

## Daily job

The launchd job `org.developmentfinance.oda-mirror-refresh` runs `refresher/run-refresh.sh` at 07:30 local time. If the Mac is asleep then, launchd runs the job at the next wake. The script appends timestamped output to `~/Library/Logs/oda-mirror-refresh.log` and posts a macOS notification when the refresher exits non-zero. Anything the script itself fails to log, such as an error before it opens the log, goes to `~/Library/Logs/oda-mirror-refresh.launchd.log`.

The refresher imports the `oda_reader` in its checkout, so `run-refresh.sh` refuses to run unless that checkout is on `main`. Keep a dedicated clone of `main` for the job, and pull it after each release. Set `ODA_MIRROR_ALLOW_BRANCH=1` to run the wrapper from another branch by hand.

Install it from the root of that clone. The plist holds placeholders for the script path and your home directory, because launchd does not expand `~`, so the command below writes a copy with both filled in into `~/Library/LaunchAgents`:

```bash
sed -e "s|/ABSOLUTE/PATH/TO/oda-importer|$PWD|" -e "s|/ABSOLUTE/HOME|$HOME|" \
  mirror/refresher/org.developmentfinance.oda-mirror-refresh.plist \
  > ~/Library/LaunchAgents/org.developmentfinance.oda-mirror-refresh.plist
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/org.developmentfinance.oda-mirror-refresh.plist
```

Start a run at once with `launchctl kickstart "gui/$(id -u)/org.developmentfinance.oda-mirror-refresh"`. The first run may ask permission to read the Keychain items. Choose Always Allow. The plist holds the checkout's path, so reinstall after moving the checkout.

Uninstall it:

```bash
launchctl bootout "gui/$(id -u)/org.developmentfinance.oda-mirror-refresh"
rm ~/Library/LaunchAgents/org.developmentfinance.oda-mirror-refresh.plist
```

## Worker

The Worker serves `GET` and `HEAD` for keys under `oecd/`, reads the bucket through the `BUCKET` binding, and answers 404 for anything else. Deploy it from `mirror/worker` after `npx wrangler login` with an account that manages the zone `developmentfinance.org`:

```bash
cd mirror/worker
npm ci && npx wrangler deploy
./smoke.sh [BASE_URL]
```

`smoke.sh` needs `curl` and `jq`. It checks that the manifest is valid JSON, that the first listed object has the manifest's size, that an unknown path returns 404, and that `PUT` returns 405. It exits non-zero on any failure, and `BASE_URL` defaults to `https://bulk.developmentfinance.org`. Run it after the first refresh, because the bucket holds no manifest before then.

## Rate limits

The Worker allows 30 file requests and 120 other requests (the manifest and everything else) per minute per client IP. A client over the limit receives 429 with `Retry-After: 60`. Cloudflare counts each limit separately in each of its locations, so the figures are approximate. They are set in `worker/wrangler.jsonc`.

## Bucket layout

Objects are `oecd/<dataset>/<sha256><extension>`, immutable and cached for a year. The manifest, `oecd/manifest.json`, is cached for five minutes and written last in each run, so a reader never sees an entry whose object is missing. The bucket keeps each dataset's current and previous object, and any object written in the last 24 hours. The bucket is private, and only the Worker serves it publicly.
