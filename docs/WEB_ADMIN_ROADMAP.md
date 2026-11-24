# Web administration migration

Reference: the approved light TGTeacher Admin designs, ten sidebar sections. React + Vite, Lucide icons, responsive CSS, same-origin Python/Starlette API. No new MCP tools. All writes must use existing domain repositories and validation, not a second content schema.

## Phase 9.1 — functional foundation

- Dashboard: real SQL counts and dated lesson activity.
- Groups: search, paging, current content, create edit draft, archive/restore, version history and rollback.
- Drafts: structured eight-stage editor with TXT/JSON fallback, revisioned save, validation, publish, soft delete; add/remove/reorder tasks and choose recent generated media.
- Learners: Telegram ID/name lookup, personal progress and review statistics in an authenticated operator-only UI, VIP extension.
- Reviews: active group vocabulary card counts, due and rated counts (not measured retention).
- Media: generated images/audio, metadata, history, preview/playback.
- TTS and Images: configured providers/models, voices, estimated cost, explicit generation confirmation, existing shared ledgers.
- Budgets: actual reservation history and configured hard daily limits. No misleading provider invoice amounts.
- Settings: web audit events and system information.
- Desktop and phone layout: drawer navigation, card rows instead of wide tables, responsive dialogs and forms.
- Dedicated single-operator credential, hashed expiring sessions, HttpOnly/SameSite cookies, origin checks, explicit mutation confirmation. No keys or learner tokens in frontend code. Separate from MCP scopes.

## Local/staging use

Copy admin.env.example to admin.env, set WEB_ADMIN_SECRET to a unique 32+ character high-entropy random secret (openssl rand -hex 32). Keep mcp.env as the shared provider model/budget configuration, chmod 600 both. Run:

    docker compose --profile web up -d --build db admin

Open http://127.0.0.1:8080. This is loopback-only. Remote access requires HTTPS reverse proxy, exact WEB_ADMIN_ORIGIN and proxy rate limiting. Do not expose the development server. Do not share the credential with learners or paste it into prompts. Changing the credential invalidates prior sessions; logout revokes the session in PostgreSQL. Use the same mcp.env values on admin and MCP processes; do not configure divergent budget limits.

## Phase 9.2 — feature parity before cutover

Polish visual editor with inline validation, searchable full asset picker, dedicated detach action and version diff. Bring ZIP import/export, bulk operations, remaining legacy user/subscription/account actions, admin messaging/help workflows, reports and operational health to the web after inventorying actual Telegram admin handlers. Use actual measured analytics, explicitly distinguish forecast and self-assessment. Add per-administrator identity/roles and richer audit attribution; migrate settings safely rather than editing arbitrary environment secrets from the browser. Add authenticated media upload and attachment UI. No unsupported provider, fictitious stats, automated quality score or dummy success controls.

## Cutover

The legacy Telegram admin remains enabled by default until parity is verified. Set TGTEACHER_LEGACY_ADMIN=0 to stop registering legacy admin handlers and hide their menu entry; optionally set TGTEACHER_WEB_ADMIN_URL=https://your-admin-domain to show the web link to existing Telegram admins. Learner bot remains intact. Test old callbacks, admin help/payment flows, role boundaries, and rollback procedure first. No production deploy or tag by this phase.
