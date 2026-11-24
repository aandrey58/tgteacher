# TGTeacher v1.0 — release candidate runbook

## Status and boundaries

45 MCP tools implemented. Stage 8 is a release audit, not a deployment. Do not merge, tag or start a production instance until the final regression and release-smoke jobs pass and the operator approves the staging checks below. Live Telegram, payment callbacks, provider billing, and actual ChatGPT authorization require separate credentials and operator-controlled staging tests.

## Prepare an isolated staging host

Use Docker Engine and Compose v2; select a distinct staging directory and Compose project name. Copy .env.example to .env, bot.env.example to bot.env, mcp.env.example to mcp.env. Restrict all three files to the operator (chmod 600); do not upload or commit them. Generate distinct high-entropy database and bearer secrets (e.g. openssl rand -hex 32). Set BOT_TOKEN to a separate staging bot token. Leave payment credentials and AI keys empty unless deliberately testing those integrations. Both generation budgets default to zero. Use only verified model IDs, formats and conservative tariffs, and set provider-side spend limits; local reservations are not invoice reconciliation.

For existing installations, stop all older bot/MCP writers before schema upgrade. Preserve the original Compose project/volume name and database credentials when upgrading; changing POSTGRES_PASSWORD does not update an existing Postgres role. Back up the database with pg_dump -Fc and separately copy the families, bot/exports, runtime_cache, admins list and legacy indexes. Restore the dump into an isolated database to verify the backup before migration. Never run docker compose down -v against production.

Create runtime_cache and bot/exports directories; place approved Telegram numeric administrator IDs, one per line, in runtime_cache/admins.txt. Existing deployments must explicitly migrate bot/admins.txt, bot/help_questions_index.json and legacy_users.json to runtime_cache. Do not reuse admin IDs embedded in source or a container image.

```sh
cp .env.example .env
cp bot.env.example bot.env
cp mcp.env.example mcp.env
chmod 600 .env bot.env mcp.env
mkdir -p runtime_cache bot/exports
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

## Release acceptance checklist

- Final GHA tests + release-smoke successful on exact candidate SHA.
- Backup and restore rehearsal against a copy of real legacy DB (never production directly).
- Review secret history / rotate exposed or old credentials; .gitignore does not remove already tracked files.
- Use staging bot to validate menus, legacy VIP/payment flows, all eight exercise stages, FSRS and admin access; test restart and revocation.
- One explicitly budgeted live call per configured TTS/image model, test output, invoice and expiry/error behavior.
- Verify reverse proxy TLS, resource metadata, OAuth consent, workspace access and independent role tests. Verify learner linking if required.
- Only then approve deployment and create the release tag. Keep backup and rollback plan.

Known limits: local budget counts reservations, not provider-side invoices; interrupted requests may remain pending and are deliberately not retried. Parallelism limits are per batch, not global provider quotas. Legacy ZIP import keeps local media rather than reconstructed generated-asset provenance; archive and version history within the same installation retain managed bytes. Older lesson callbacks within the same version are not all single-use (SRS tickets are). External OAuth identity binding, live payments, actual deployment and full security penetration test are not certified by CI.
