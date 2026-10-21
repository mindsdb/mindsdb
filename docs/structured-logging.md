# Stack diagnostics

Compose defaults the API and web services to WARNING with errors-only access.
The API uses the server's installed logging config. The web image uses the
frontend's native nginx JSON access format and safe error classifier. GET/HEAD
`/healthz` successes are quiet; failures remain visible.

This stack builds from submodule source. Its gitlinks must include the matching
frontend, server, Anton and data-vault changes before claiming the full stack is
migrated. Rebuild images after updating those gitlinks. Installed packages and
snapshot images outside this build remain independent release gates.

Run `make docker-build` after initializing the submodules and fetching their tags.
This uses each package's Hatch build backend to derive its version from the pinned
source, then passes both versions into Docker. The API builds separate wheels so
the server's Anton dependency resolves to that source. Docker cannot resolve Git
versions from the copied submodule directories and must not use their fallback
versions. Direct Compose builds require `ANTON_VERSION` and `COWORK_SERVER_VERSION`
from the same `uvx --with hatch-vcs hatchling version` command in each submodule.
The pinned server exposes `/api/v1/health/`; Compose readiness and nginx's API
prefix must match it. Older pins used different routes, so refreshing source
gitlinks also requires checking the serving configuration.

Verify the built images in disposable containers bound only to loopback:

```bash
COMPOSE_PROJECT_NAME=logging-review make docker-build
python3 scripts/test-compose-logging.py \
  --api-image logging-review-api --web-image logging-review-web
```

The smoke test checks the image's health command, a JSON API response through
nginx, quiet successful probes, failed readiness and request-value exclusion.
It removes only its own containers and does not mount host data. The data-vault
gitlink is included in the source bundle but this two-service Compose stack does
not run that service; validate its installed deployment separately.

Set LOG_LEVEL=DEBUG and ACCESS_LOG_MODE=all only for an owned investigation; restore
WARNING/errors and recreate the affected services afterward. Preserve the prior
image versions and compose settings for rollback. Verify the live collector and
retained audit streams before production rollout.

Diagnostics use `logged_at`, `logger`, `log_level`, and `message`. Explicit events,
reasons, source locations and correlation fields identify failures. Exception
values, source lines, locals, arbitrary extras, HTTP queries and provider bodies
are excluded. Use the structured fields when querying logs; the previous text
prefixes are no longer a parsing contract.

Enable extra detail only for an owned troubleshooting session, repeat the failing
operation, and restore the default afterward. Logging controls do not enable
framework debug responses. Roll back a release by restoring its previous image
or package version and its previous logging settings together. Before promotion,
verify collector parsing, saved searches, alerts and retained operational streams
against the emitted records. A passing unit suite does not establish deployment
or collector compatibility.
