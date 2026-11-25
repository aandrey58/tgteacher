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

## Phase 9.2 — reference design and content workflow

- Reference-style light layout, section KPI cards, group table with status/access filters and sorting, publishing timeline and responsive media gallery/list. Search uses the database before pagination; group search includes vocabulary and translations.
- Eight-stage visual editor with revision validation, searchable paginated asset picker, detach action and stable-ID version comparison; source editor remains available.
- React navigation protects against stale responses; search controls remain mounted during same-page requests. No fabricated production metrics.
- Browser acceptance: all ten sections plus editor screenshots at 1536 and 390 px, create/edit/validate/publish, group filtering, version comparison, logout, no horizontal document overflow or browser exceptions. Synthetic fixture data is confined to the isolated CI database. CI artifacts retain screenshots for manual review.

## Phase 9.3 — mobile UX

Russian UI remains the default. Compact layout uses drawer navigation through 900 px, two-column record cards on phones, three-column record cards on tablets, 44 px primary touch targets and 16 px form inputs. Native dialog top layer contains keyboard focus, blocks background scrolling, restores focus after closing, and preserves the existing confirmation for unsaved changes. Phone editor fills the viewport and keeps revision actions sticky; media gallery and version comparison remain responsive.

Acceptance: complete authenticated workflow and all ten sections at 320, 375, 390, 430, 768 and 1536 CSS px in Chromium, plus 390 px WebKit touch emulation. Check navigation, focus restoration, rejected discard, translation persistence, all eight stage tabs, overflow, publication, version comparison, logout and browser errors. CI stores actual screenshots. WebKit emulation is not a physical iPhone test; native keyboard/safe-area behavior still needs staging on real devices.

## Phase 9.4 — feature parity before cutover

Inventory: admin_panel (groups, template, archive, ZIP export, broadcast, status/logs/snapshots); admin_users and nested search/progress/inactive handlers; sub_pay and sub_gift (tariffs, exact-day gifts, revoke, extend active subscriptions, payment history); private_message (text/photo/video/document); help (group reply index); admin_list (Telegram IDs).

Implemented web equivalents:
- Groups: bounded standard ZIP import to an unpublished draft, TXT template, ZIP export of current version, atomic bulk archive/restore.
- Learners: search, selected bulk gift/revoke, single account deletion, detailed current task answers, exact-day gifts with zero-amount payment history and operation-ID replay guard. Existing 45 MCP tools unchanged.
- Subscriptions: four tariffs maximum, 1–60 months and 1–100000 RUB, active subscriber extension by days with count confirmation.
- Communications: persistent recipient snapshots, text/photo/MP4/approved documents, single/multi-recipient delivery, draft preview/confirm/cancel, per-recipient status; worker runs in polling bot with no token in web container. Ambiguous sends never automatically retried. Global broadcasts exclude legacy admins.
- Support: text question persistence before acknowledgment, text replies, close, queue status. Enable TGTEACHER_WEB_SUPPORT=1 only once web support is staffed. Existing legacy questions remain in the Telegram help index, not silently imported. Legacy mode remains available until acceptance. Historical index import is now an explicit operator dry-run/apply command; recovered questions are archived because historical answers/status are not stored. Support replies can use the same validated image/document/MP4 attachment pipeline as communications.
- Reports: read-only XLSX exports and preview for learners/new/inactive, group progress, database payment history, system snapshots. Date filters for new/payments/system, relative inactivity period. No live payment refresh or spreadsheet formula injection.
- Settings: bot-owned snapshot requests, auto-snapshot control, latest measured metrics; queued revision-checked atomic updates to legacy Telegram administrator IDs. Web credential stays independent. Existing web audit log remains available.
- Media: bounded PNG/JPEG/WebP/MP3 uploads to shared immutable asset tables; message documents/video are separate, opaque attachments; no inline execution or arbitrary external URL downloads.

Limits and acceptance gates: ZIP is limited to 32 MiB compressed, 64 MiB expanded, 501 entries, flat or one-level lesson directory and UTF-8 TXT; legacy CP866/CP1251 filenames are recovered only when the standard interpretation fails to resolve referenced media; ambiguous recovery is rejected. Uploaded message documents/video <=16 MiB. Historical Telegram help attachments and raw bot logs are not migrated. Settings expose a separate bounded warning/error journal with only UTC timestamp, fixed severity and component category; no raw messages, logger names, user IDs, tokens or traces. Old raw logs stay outside the web interface. Full external payment reconciliation, physical device testing, independent per-admin identity and final cutover remain separate. No production deploy or real Telegram send in CI. Run backend and all browser projects before declaring complete.

### Historical support migration (operator-only, no automatic sends)

Back up the legacy JSON index and database, stop legacy group replies and ensure the web schema has initialized. In the bot runtime run `python -m scripts.import_legacy_help --group-id <legacy-negative-chat-id>` to preview counts, then add `--apply` after review. Use the original group ID consistently. Existing users only, archived questions with explicit unknown answer status, original timestamp text retained, idempotent group/message key. Missing users are counted, not recreated. The old index is not deleted; historical responses and attachments cannot be recovered from that index. Do not execute against production during CI.

## Phase 9.5 — security and reliability

Individual web accounts and server-enforced owner/editor/support roles. Passwords use salted PBKDF2-SHA256 (600,000 iterations), minimum 16 characters; account changes revoke sessions, revision checks prevent stale edits, and the last enabled owner cannot be disabled or demoted. Existing shared-key sessions are invalidated at migration. Sign in with the existing key (blank login), create and test a personal owner in Settings, then optionally set WEB_ADMIN_ALLOW_RECOVERY=0 and restart admin. A startup guard requires an enabled owner. Keep the recovery key on the server. Require HTTPS and reverse-proxy login throttling in production; the app also applies a process-local 120/min login limit. Owner manages all sections; editor manages content/media/budgets and sees aggregate learning stats, not learner records or settings; support sees questions and support replies only, with media selection, not global broadcasts. The audit records actor/role, login/logout and denied operations without storing passwords. Concurrency hardening: active VIP extensions use a recipient-set fingerprint and single-use operation ID, and stale support cancellations cannot reopen a replacement reply. Delivery completion changes support state only if it still owns the sending claim. Generation ledgers keep a consistent UTC reservation timestamp within each locked batch; zero-cost cache hits remain usable even after a limit reduction. Campaign pagination selects 51 campaigns before aggregating deliveries; supporting indexes cover campaign history, support order and learner deletion. Existing concurrent generation tests plus new replay, changed-selection, lowered-limit, and support-state regression checks run in CI. Account creation and changes recheck the actor under the account transaction lock, so a demoted owner cannot create another owner using a stale authorization snapshot. Browser checks exercise editor/support login, hidden sections, direct API denial, support draft recovery and cancellation on desktop, 320px Chromium and WebKit. Analytics use the same access/archival predicates as learner SRS; dashboard lesson activity excludes historical snapshots and uses UTC dates. Budget totals use bounded UTC-day intervals. Group/learner tables page before expensive progress/card counts; indexes support activity, registration, answer timestamps, review timestamps and family progress. Synthetic scale and metric assertions are part of CI (see tests/test_web_scale.py). This is a regression workload, not production capacity certification.

## Phase 9.6 — acceptance and cutover

Run full regression and browser workflows, compare screenshots against the approved references, verify feature parity and rollback plan, then disable legacy admin handlers. Stale administrative callbacks now receive an explicit retired/denied response in web-only mode. Learner noop pagination remains registered independently. Legacy group support replies are not registered in web-support mode; legacy-mode replies require an approved Telegram administrator. The registration switch and rollback, menus, background jobs and persistence-before-acknowledgment are covered by isolated tests. Message attachments now include MP3, OGG Opus voice and static WebP stickers, with bounded validation, idempotent upload, one confirmed queued send and persisted filename/type in campaign history. Sticker captions are rejected rather than silently discarded. Animated and video stickers remain a documented exception. See [cutover checklist](WEB_ADMIN_CUTOVER.md) for deliberate differences and remaining release gates. No unsupported provider, fictitious stats, automated quality score or dummy success controls.

## Cutover

The legacy Telegram admin remains enabled by default until parity is verified. Set TGTEACHER_LEGACY_ADMIN=0 to stop registering legacy admin handlers and hide their menu entry; optionally set TGTEACHER_WEB_ADMIN_URL=https://your-admin-domain to show the web link to existing Telegram admins. Learner bot remains intact. Test old callbacks, admin help/payment flows, role boundaries, and rollback procedure first. No production deploy or tag by this phase.
