# TGTeacher v1.0 — release candidate runbook

## Status and boundaries

45 MCP tools implemented. Stage 8 is a release audit, not a deployment. Do not merge, tag or start a production instance until the final regression and release-smoke jobs pass and the operator approves the staging checks below. Live Telegram, payment callbacks, provider billing, and actual ChatGPT authorization require separate credentials and operator-controlled staging tests.

## Stage 10 release blockers

See [Stage 10.1 architecture audit](STAGE_10_RELEASE_AUDIT.md) for confirmed source-level defects and the remediation sequence. In particular, do not enable real YooKassa payment creation or approval of production while PAY-01..03 remain unresolved. Green web-admin CI is not payment integration certification.

## Prepare an isolated staging host

Use Docker Engine and Compose v2; select a distinct staging directory and Compose project name. Copy .env.example to .env, bot.env.example to bot.env, mcp.env.example to mcp.env, and admin.env.example to admin.env if enabling web admin. Restrict all files to the operator (chmod 600); do not upload or commit them. Generate distinct high-entropy database and bearer secrets (e.g. openssl rand -hex 32). Set BOT_TOKEN to a separate staging bot token. Leave payment credentials and AI keys empty unless deliberately testing those integrations. Both generation budgets default to zero. Use only verified model IDs, formats and conservative tariffs, and set provider-side spend limits; local reservations are not invoice reconciliation.

For existing installations, stop all older bot/MCP writers before schema upgrade. Preserve the original Compose project/volume name and database credentials when upgrading; changing POSTGRES_PASSWORD does not update an existing Postgres role. Back up the database with pg_dump -Fc and separately copy the families, bot/exports, runtime_cache, admins list and legacy indexes. Restore the dump into an isolated database to verify the backup before migration. Never run docker compose down -v against production.

Create runtime_cache and bot/exports directories; place approved Telegram numeric administrator IDs, one per line, in runtime_cache/admins.txt. Existing deployments must explicitly migrate bot/admins.txt, bot/help_questions_index.json and legacy_users.json to runtime_cache. Do not reuse admin IDs embedded in source or a container image.

```sh
cp .env.example .env
cp bot.env.example bot.env
cp mcp.env.example mcp.env
cp admin.env.example admin.env
chmod 600 .env bot.env mcp.env admin.env
mkdir -p runtime_cache bot/exports families
# After taking the backup and stopping old writers, on the intended staging host:
sudo chown -R 10001:10001 runtime_cache bot/exports families
sudo chmod -R u+rwX runtime_cache bot/exports families
# Fill credentials and administrator list before running.
docker compose config --quiet
docker compose build
docker compose up -d --wait db mcp
curl --fail http://127.0.0.1:8000/healthz
# Only after staging verification:
# docker compose up -d bot
```

The DB has no published host port. MCP is bound to 127.0.0.1 on the host, not public. The bot and MCP share the database and lesson files; the MCP mount is read-only. Credentials are separated by service. The image build context excludes runtime data, secrets and the administrator list. Backups include PostgreSQL audio/image bytes, FSRS states and history. Initialization takes a database advisory lock and runs in a transaction. Do not start the previous binary against the upgraded schema. Rollback of a failed software release means restore the pre-upgrade DB and file backup (with writers stopped), not merely switching the image. Export/import of lesson ZIPs does not migrate learner history; imported media is renamed to local legacy files, not restored as the original generated-asset provenance.

## MCP smoke test

In a client that supports Streamable HTTP and bearer authentication (MCP Inspector, local SDK or Responses API remote MCP), connect to http://127.0.0.1:8000/mcp using one of the local test credentials. Inspect tools/list (45 tools), call list_families, test a draft lifecycle with editor/publisher credentials, archive/restore and rollback. Never include raw tokens in prompts, logs or screenshots. Learners use /review_token in the private bot chat to obtain a revocable 24-hour bearer token; /review_revoke revokes it. Test with two learner identities. This static bearer path is for clients supporting explicit bearer credentials, not a claim that the ChatGPT UI accepts a manually pasted bearer token.

## ChatGPT connection (OAuth required for protected tools)

Official references, checked 2026-11-24:
- https://developers.openai.com/plugins/build/auth
- https://developers.openai.com/plugins/deploy/connect-chatgpt
- https://help.openai.com/en/articles/12584461-connecting-custom-mcp-servers-in-chatgpt

A remote ChatGPT connection needs an accessible HTTPS MCP URL (or supported Secure MCP Tunnel). Custom server access and write actions depend on account/workspace eligibility and policies; verify this in the user's actual ChatGPT workspace. Do not select unauthenticated access for TGTeacher.

TGTeacher implements a resource server, not an authorization server. For content-administrator OAuth, provision an external authorization server supporting OAuth authorization code + PKCE, discovery metadata, suitable client registration (CIMD, DCR or provided OAuth client per OpenAI docs), token issuance, refresh where applicable, and RFC 7662 introspection. Configure scopes content:read, content:edit and content:publish according to actual roles, not all by default. Introspection must return active=true, aud containing the exact HTTPS MCP resource URL, integer exp and scope. Use HTTPS and service credentials for introspection.

In mcp.env set TGTEACHER_MCP_AUTH_MODE=oauth, TGTEACHER_MCP_RESOURCE_URL=https://your-domain.example/mcp, TGTEACHER_MCP_ISSUER_URL to the exact OAuth issuer, TGTEACHER_MCP_INTROSPECTION_URL and the introspection client credentials. Put a TLS reverse proxy in front of localhost:8000, preserve Host, allow MCP GET/POST/DELETE and discovery routes, disable response buffering, cap request size/rate and set appropriate timeouts. Do not expose the DB or an unauthenticated proxy bypass. Verify protected resource and OAuth authorization server metadata, then test OAuth using MCP Inspector.

In an eligible ChatGPT workspace follow the official connection instructions: create a custom MCP app/plugin connection, supply the HTTPS /mcp URL and OAuth configuration, authorize a test account, refresh tool discovery and verify allowed actions. Test reader, editor and publisher roles separately; ensure consent for publishing and paid generation. Do not use "No authentication". Learner Telegram bearer credentials are not integrated into the external OAuth flow; actual ChatGPT learner account linking is a release blocker for that workflow. Existing OAuth introspection intentionally strips learner scopes, so a client_id cannot impersonate a Telegram user. A separate authenticated binding between OAuth subject and Telegram account must be implemented before learner OAuth is enabled.

## Web administration cutover

See [Web admin cutover checklist](WEB_ADMIN_CUTOVER.md). Switch only after the web owner and support team have accepted staging. Mode rollback is distinct from reverting binaries/schema: with the same current release, restore legacy flags and recreate the bot container, keeping PostgreSQL and web workers intact. Never drop data to roll back an interface switch.

## Release acceptance checklist

- Final GHA tests + release-smoke successful on exact candidate SHA.
- Backup and restore rehearsal against a copy of real legacy DB (never production directly).
- Review secret history / rotate exposed or old credentials; .gitignore does not remove already tracked files.
- Use staging bot to validate menus, legacy VIP/payment flows, all eight exercise stages, FSRS and admin access; test restart and revocation.
- One explicitly budgeted live call per configured TTS/image model, test output, invoice and expiry/error behavior.
- Verify reverse proxy TLS, resource metadata, OAuth consent, workspace access and independent role tests. Verify learner linking if required.
- Only then approve deployment and create the release tag. Keep backup and rollback plan.

Known limits: local budget counts reservations, not provider-side invoices; interrupted requests may remain pending and are deliberately not retried. Parallelism limits are per batch, not global provider quotas. Legacy ZIP import keeps local media rather than reconstructed generated-asset provenance; archive and version history within the same installation retain managed bytes. Older lesson callbacks within the same version are not all single-use (SRS tickets are). External OAuth identity binding, live payments, actual deployment and full security penetration test are not certified by CI.

## SEC-01 container migration

Application images run as UID/GID 10001; Compose drops all capabilities and enables no-new-privileges. Before upgrading an existing installation, stop writers, verify backups, and ensure the three bind-mounted directories are owned/writable by UID 10001. The application code remains root-owned and non-writable. Do not apply recursive permission changes to unrelated directories or the PostgreSQL volume. Raw bot logs now live in runtime_cache/log.txt under Compose; existing historical logs are not automatically removed. Preserve access for the operator through appropriate host permissions.

The admin service no longer reads mcp.env. If web generation is used, copy ONLY approved AI provider configuration (see admin.env.example) to admin.env. Do not copy MCP bearer tokens, OAuth client secrets or BOT_TOKEN. Use separate provider credentials where supported; keep identical approved model/pricing/budget definitions across services to avoid inconsistent accounting. Both budgets default to zero. Check that generated media features work in staging before production; never print the resolved Compose environment or credentials to logs.

The legacy tracked bot/admins.txt is removed from this branch, not rewritten from Git history. Do not restore it from source; use a private runtime administrator list. Historical commits and main are untouched. Database roles remain shared; this change does not provide database least privilege or a complete secrets audit.

## PostgreSQL role isolation (SEC-01, staging candidate)

Use a dedicated TGTeacher database and stop all writers before changing roles or schema. The base compose remains a legacy compatibility mode and is NOT the hardened production candidate. The opt-in overlay removes the owner credential from runtime containers. Back up and restore-test the DB first. Set three distinct random ASCII passwords in .env (at least 24 characters, e.g. openssl rand -hex 32). Never reuse the owner password. Then on the intended staging host:

```sh
export COMPOSE_FILE=docker-compose.yml:docker-compose.isolated.yml
docker compose up -d --wait db
docker compose --profile migration run --rm --build db-migrate
docker compose --profile web up -d --build mcp admin
# Start bot only with approved staging token:
# docker compose up -d bot
```

Run the one-shot migrator again before starting an upgraded schema version; do not keep a privileged service running. Runtime services refuse startup on wrong role or schema-release marker. Migration requires the existing database administrator, public schema, and dedicated database; it revokes PUBLIC table/sequence privileges and TEMP on this database. Do not use on a shared application database. Grants are explicit per table; future schema/table changes require updating the allowlist and incrementing the schema release marker. No automatic rollback to owner credentials.

Bot retains broad content/learner/payment/queue writes for legacy administration; admin retains content/learner/queue/auth writes, payment reporting and legacy gift ledger writes; MCP has content/review/progress writes (publication remaps progress), read access to learner records/tokens, and no payment, support, web identity or snapshot table access. This is service isolation, not per-learner SQL row-level security. Existing paid ledger policy and staging backup/restore remain unchanged. Database-level PUBLIC CONNECT to other databases, superuser administration and provider-side auth are outside this slice.

## Dependency and secret audit

The separate Security and dependency audit workflow generates redacted history/current-tree findings and dependency evidence. A scanner failure is not a clean result. Only metadata (rule, file, line, commit and fingerprint) is retained for secret findings; raw scanner output is not uploaded. Treat findings as unverified until reviewed privately, rotate any confirmed exposed credentials before attempting history cleanup, and coordinate history rewriting with the owner. The workflow scans reachable fetched refs, not deleted/unreachable commits or external services. Do not use a broad ignore rule to greenwash CI.

Review candidate requirements.lock and package-lock.json from the first real dependency job, including advisory reports and successful locked install/build evidence. Commit only verified locks, then make both Dockerfiles and regression CI enforce them. No production deployment is approved by this bootstrap audit. Python advisories and npm high/critical advisories fail the dedicated job; lesser npm findings are retained for review.


## BUILD-01: verified lock enforcement

Imported exact artifacts from successful audit 45728975647 after SHA-256 verification (import run 45730237097, commit 0bafaf6). Python and npm locks are committed; Docker and regression CI use hash-verified pip and npm ci without unlocked fallback. Python and Node base images are pinned to digests observed in release-smoke 45728975721. Regenerate locks deliberately on Python 3.12 / Node 22 and review the security audit before committing changes. PostgreSQL image digest, apt package reproducibility and OS/container CVE scan remain open. This is not bit-for-bit build reproducibility. Fresh CI confirmation and staging acceptance are required.


## BUILD-01: PostgreSQL digest and release image scanning

Pin PostgreSQL 16 in Compose and the regression service to the exact registry digest recorded during successful run 45730519558 (job 139611083839). The release-smoke job now scans the actual built bot, MCP, admin, one-shot database migrator and the pinned database image with versioned Trivy. It saves each local image to an archive and scans without mounting the Docker socket. JSON reports include package inventories, all severities and local image IDs; HIGH/CRITICAL findings (including unfixed findings), missing scan targets and scanner errors fail the job. No advisory allowlist or ignore-unfixed switch is applied. Scanner output and findings require review; lower severity findings are retained. This does not attest production or guarantee bitwise reproducibility: apt repositories, scanner database and other CI tooling evolve. Follow-up must review real results, patch base images or OS packages as appropriate, and promote the same tested image digests in staging rather than rebuild. No production changes.


## BUILD-01: first OS remediation

Trivy run 45731389231 found 322 HIGH / 10 CRITICAL package-advisory records per application image, 99 HIGH / 2 CRITICAL for PostgreSQL. These are not counts of distinct CVEs or verified reachable exploits. Most OS findings had no fixed version listed. The first remediation installs available Debian updates and asserts libzstd1 >= 1.5.7+dfsg-1+deb13u1 (CVE-2026-124851); the database derivative also updates zstd and retains the original pinned PostgreSQL 16 base, entrypoint and volume. Build db explicitly before starting or upgrading it (docker compose build db); existing regression service remains the upstream digest, while release smoke uses the patched derivative. The migration-only build target excludes FFmpeg and its OS dependencies; app services retain media functionality and gain a synthetic Opus encode/probe smoke check. Go runtime advisories in upstream PostgreSQL helper binaries and Debian advisories without available fixes remain under investigation. The HIGH/CRITICAL gate is unchanged, and a new report is necessary before claiming remediation success. No production deployment.
