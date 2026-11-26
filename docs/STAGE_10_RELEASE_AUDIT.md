# Stage 10.1 — архитектурный аудит кандидата TGTeacher

Дата: 25.11.2026. Проверенная ветка: english-adaptation, SHA db4589a064ea240c5d183b3ff4d613b4ec8c4fd6. Это аудит архитектуры и критических путей, не полный построчный security review или сертификация production.

## Решение

NO-GO для реальных платежей и production cutover. GO для продолжения разработки и изолированного staging без реальных платежей/платных генераций. Все 45 MCP-инструментов реализованы по roadmap; не требуется добавлять новые инструменты для исправления дефектов. Stage 9.6 проверял интерфейс и регистрацию, а не реальное зачисление оплаты.

## Объём и базовая регрессия

- Рекурсивное дерево: 150 отслеживаемых файлов, выдача не усечена; изучены roadmap, критические реализации Telegram/БД, контентные версии, FSRS, MCP-аутентификация, web-admin, очередь, поставщики медиа, платежи, legacy-задачи, Docker и конфигурация.
- Все 12 Python test-модулей просмотрены по инвентарю тестов: 99 test-методов. В них нет прямых тестов YooKassaPayment, Payment.create/find_one, активации и возврата оплаты. Имеющиеся проверки подарков и отчётов не равны проверке интеграции.
- [GHA 45663564120](https://github.com/aandrey58/tgteacher/actions/runs/45663564120), [45663551597](https://github.com/aandrey58/tgteacher/actions/runs/45663551597): оба успешны на проверенном SHA. Предыдущий [45657039120](https://github.com/aandrey58/tgteacher/actions/runs/45657039120): 99 backend, 10 browser, 4 условно пропущены, 45 MCP, synthetic p95 max 403.2 ms (5000 пользователей). Это не нагрузка реального polling или YooKassa.

## Состояние компонентов

| Компонент | Подтверждённое состояние | До релиза |
|---|---|---|
| Английский контент 1–8 | Парсер, валидация, TXT/ZIP, задачи, сохранение прогресса | Сквозные сценарии Telegram, проверка повторов и фактических материалов |
| Версии/архив | CAS, стабильные ID, история, rollback, DB advisory lock | Rehearsal миграции реальной копии |
| FSRS / адаптация | Реальные ratings, tickets, self-assessed CEFR, scoped learner tokens | Проверка срока VIP, удаления, legacy-импорта; реальные устройства |
| MCP | 45 tools, проверяемые роли, локальные bearer и external introspection | External OAuth в staging; learner OAuth binding не реализован |
| TTS / Images | Провайдеры, immutable assets, кэш и UTC reservation | Проверка моделей, лимитов и счетов; глобальная квота |
| Web admin | React, роли, журнал, очередь, mobile, CI switch/rollback | Оператор, референсы, staging; исключения TGS/WEBM и raw logs |
| Платежи / VIP | Старый YooKassa SDK, проверка пользователем, подарки, тарифы, отчёт БД | Критические исправления P0/P1 ниже |
| Инфраструктура | Раздельные bot/MCP/admin, DB не опубликована, loopback MCP/admin | Lockfiles, непривилегированные контейнеры, TLS, secrets, health/monitoring, backup restore |

## Реестр дефектов и рисков

Приоритет: P0 — блокирует денежные операции/релиз; P1 — исправить или явно закрыть контролем до production; P2 — ограничение/технический долг. «Код» означает дефект подтверждён чтением реализации, а не воспроизведённым реальным платежом. Никаких живых платежей не создавалось.

| ID | Приоритет / доказательство | Проблема | Критерий исправления |
|---|---|---|---|
| PAY-01 | P0, код: services/payments/yookassa_payment.py:111–205 | INSERT processed=true до продления VIP на другом соединении; повтор INSERT конфликтует с UNIQUE. При сбое между операциями оплаченная подписка может остаться без доступа, повтор статуса не восстанавливает её. | Единственная DB-транзакция с блокировкой заказа/пользователя; повтор succeeded возвращает тот же результат; fault injection на каждом шаге, параллельный повтор не удваивает срок. |
| PAY-02 | P0, код: yookassa_payment.py:33–105, 222–250; handlers/user/sub.py:111–149 | Срок и цена передаются из callback, перед созданием не сверяются с payment_options; локальный заказ до API не фиксируется. Устаревшая кнопка может создать платёж по прежней цене; неизвестный исход запроса не имеет durable idempotency key. | Серверный тариф, неизменяемый заказ до SDK, ключ идемпотентности, явная обработка stale quote и неопределённого исхода без слепого повтора. |
| PAY-03 | P0, код: yookassa_payment.py:111–205, 277–301 | Проверка не получает ID вызывающего ученика, использует provider metadata без сверки с сохранённым заказом, суммы через float; нет сверки валюты и назначения. Другой владелец может проверить чужой payment ID; надёжность атрибуции не доказана. | Владелец локального заказа, точная сумма в копейках, RUB, назначение/metadata, provider ID, paid/succeeded; запрет несоответствий до начисления; mock-тесты. |
| PAY-04 | P1, код: yookassa_payment.py:55–95, 122, 174; core/app.py:278–302 | Синхронные SDK-вызовы в async handler, фиктивный email в чеке, фиксированный vat_code, default return_url; нет зарегистрированного reconciliation job. | Ограниченный по времени неблокирующий SDK, реальный согласованный чек/НДС/контакт/URL, контролируемая сверка pending и legacy оплат, policy возврата; отдельный staging. Фискальные параметры согласует мерчант. |
| VIP-01 | P1, код: db/user_repo.py:769–838; db/web_operations.py:111–144; handlers/user/families.py:69–78 | Legacy getter возвращает флаг без срока, а SRS/web/learning guard проверяют expiry. Меню может обещать VIP, который блокируется при открытии. Legacy продление read-modify-write без row lock; ошибочный INTERVAL '$2 months' в ветке отсутствующего пользователя. Подарок и покупка используют разные правила счётчика и бессрочного доступа. | Единый предикат активного VIP, согласованная семантика бессрочного доступа/30 дней/счётчика, атомарный update, тест expired/revoked/perpetual и конкуренции. |
| VIP-02 | P1, код: services/payments/sub_gift.py:145–233 | Legacy подарок обновляет users и затем пишет payments без одной транзакции; секундный payment ID допускает коллизию. | Общий сервис начислений с операцией UUID и транзакцией, либо контролируемое отключение старых mutators после staging. |
| SUP-01 | P1, код: handlers/user/help.py:206–321 | Legacy support удаляет сообщение и подтверждает получение до отправки в группу. Ошибка отправки/конфигурации оставляет вопрос потерянным; флаг уже снят. Web-путь сохраняет до ACK. | ACK только после надёжной записи/доставки, оставить вопрос при сбое и не передать его другим текстовым handlers; regression в legacy и web режимах. |
| DATA-01 | P1, риск из кода: services/legacy/legacy_users_collector.py:44–130; db/user_repo.py:873–891 | Удаление пользователя не очищает PTB user_data и legacy sources, периодическая сборка может восстановить профиль. Payments намеренно остаются без FK; политика хранения финансовой истории не описана. | Тест удаления + scheduled collectors; предотвратить нежелательное повторное создание, явно определить хранение/анонимизацию платежей. |
| CONC-01 | P1, риск из кода: core/learning_session.py:42–101, db/content_versions.py:366–406, db/pool.py:8–29 | Shared advisory lock и DB connection удерживаются на весь Telegram handler, включая сетевые await. Медленная сеть может задержать публикацию; ленивый pool без lock может создать несколько pools при одновременной инициализации. | Reproduce latency, ограничить время/область lock без потери version fence; serialized pool initialization, test shutdown and concurrent start. |
| LEARN-01 | P1, известное ограничение: core/learning_session.py, RELEASE_RUNBOOK | Версионный fence защищает устаревшие версии, не делает каждый callback внутри версии одноразовым; completion_count/answer events могут изменяться при повторе. | Сквозные 8 этапов, replay/race/restart тесты; одноразовые или CAS state transitions там, где меняют прогресс, не менять смысл исторических данных. |
| STAT-01 | P2, код: db/user_repo.py:867; handlers/user/sub.py:subscription_callback | Legacy агрегат исключает stage7; экран подписки жёстко обещает 3 месяца/300 ₽ несмотря на динамические тарифы. | Согласовать метрики stage1–8 и актуальные тарифы, не показывать несуществующую цену. |
| AI-01 | P1, код: db/tts_repo.py, db/image_repo.py | Семафоры на invocation, не общие между MCP/web; локальные reservations не фактические счета. Unknown/pending не имеют автоматического reconcile. | Глобальная квота или проверенный внешний limit, операторский разбор pending, provider caps; не повторять неопределённый платный запрос автоматически. |
| AUTH-01 | P1, документированное ограничение: mcp_auth.py, RELEASE_RUNBOOK | External OAuth content introspection не связывает subject с Telegram learner; персональный learner MCP использует отдельный 24h bearer. | Реальная OAuth проверка content roles, решение о необходимости learner OAuth; не выдавать learner scope без binding. |
| BUILD-01 | P1, дерево/конфиг | Нет npm lockfile и Python resolved lock, Docker bases mutable; npm install и открытые Python ranges делают rebuild неповторяемым. | Проверенные lockfiles, npm ci, hashes/locked versions, SBOM/dependency audit, same-image staging/production. |
| SEC-01 | P1, конфигурация | Dockerfiles без USER, bot/admin/MCP используют общую БД с одинаковыми credentials; admin загружает mcp.env. bot/admins.txt присутствует в git tree несмотря на ignore. История/содержимое секретов не сканировались. | Изоляция secrets/roles, least privilege, non-root, аудит истории и ротация обнаруженных секретов. Не публиковать значения в отчёт. |
| OPS-01 | P1, конфигурация: docker-compose.yml, core/app.py | DB/MCP имеют healthcheck, bot/admin нет; реальные job queue failures не проверены uptime мониторингом. | Health/readiness, мониторинг очереди и last-success job, backup/restore, retention, аварийный runbook, staging. |
| UX-01 | P2, известные исключения 9.6 | Нет TGS/WEBM, исторических вложений, raw-log просмотра; ручное сравнение оригинальных референсов и устройств не состоялось. | Явная приёмка исключений и staging; raw logs не открывать через web. |

## Stage 10 — план после аудита

1. 10.2a — **сначала платежный блокер PAY-01..03 и VIP-01/02**. Транзакционная модель заказа, миграция legacy, callback ownership, идемпотентность, тесты на mock SDK + PostgreSQL; не запускать оплату до прохождения тестов. PAY-04: согласовать fiscal policy, reconciliation/refunds, не подменять неизвестные бизнес-правила.
2. 10.2b — безопасность: SEC-01, BUILD-01, SUP-01, DATA-01, AUTH-01; реальные SCA/secret scan, regression RBAC. Не считать этот отчёт пройденным security audit.
3. 10.3 — эксплуатация: OPS-01, CONC-01, AI-01, воспроизводимые сборки, контроль budgets/queue, backup restore, конфигурация staging.
4. 10.4 — отдельное staging, импорт копии реальных данных, проверка 8 этапов/LEARN-01, платежные sandbox credentials, роли, все провайдеры с утверждённым бюджетом; STAT-01.
5. 10.5 — интеграционная приёмка Telegram, OAuth, платежи и провайдеры, физические устройства.
6. 10.6 — переключение/откат на staging, затем production только при явном разрешении и закрытии blockers.
7. 10.7 — стабильный release tag и неизменяемый образ после успешной приёмки.

## Границы

Не выполнены: полноценный построчный review всех файлов и git history, vulnerability scan зависимостей, pentest, real-DB restore, реальные платежи/возвраты, live provider/OAuth, physical-device acceptance. Проблемы выше найдены статическим анализом, не воспроизводились на production. Новых багов при выполнении статического аудита не исправляли: платёжная архитектура и миграция требуют целевого тестирования. main, production, теги и credentials не изменены.

## 10.2a — first payment remediation slice

New orders use a server-side 30-minute quote and UUID, revalidate the current tariff before a durable creation claim, pass the same UUID as the SDK idempotency key and never automatically repeat uncertain creation. Owner/amount/currency/type/metadata/provider ID/paid are checked; settlement updates VIP, payments ledger and order state in one transaction. Existing paid ledger rows are preserved; legacy payment IDs are not auto-granted because old records may be inconsistent. New purchase creation is disabled by default and requires explicit merchant-approved external fiscalization plus an HTTPS return URL. No fabricated email or VAT code. Remaining: operator reconciliation of uncertain/legacy orders, refunds, real receipt workflow if external fiscalization is unsuitable, legacy gift unification, live staging and CI confirmation. Permanent VIP is retained by the new grant path and expiry is checked in learner menus. Counter remains purchased months vs gift operations as historically defined; no silent migration of existing counts.

### 10.2a — reconciliation, refunds and platform policy

Operator-only CLI lists active/refunded orders, verifies existing provider IDs with authenticated lookup, supports preview and explicit apply for matching durable orders, and inspects legacy records read-only. No charge/refund creation or automatic legacy credit. Refund amount is the provider's verified cumulative RUB total, recorded monotonically with observation history. A refund observed before grant blocks automatic credit; after credit, access is unchanged pending operator review (cannot subtract blindly from mixed purchases/gifts). Partial and full returns require explicit commercial policy; no invented pro-rata rules. Source: https://yookassa.ru/developers/payment-acceptance/after-the-payment/refunds. Legacy gifts now route through the web transaction service, with UUID confirmation and rejection of obsolete callbacks.

**New release gate: Telegram digital goods must use Stars.** Official policy: https://core.telegram.org/bots/payments-stars. New in-bot RUB purchases and external checkout links are disabled regardless of the YooKassa creation flag; old orders remain checkable. The service-level YooKassa adapter is retained for isolated testing and a separately designed independent checkout, not a Telegram purchase bypass. Stars invoice/pre-checkout/successful-payment/refund flows, XTR tariff configuration and operator policy remain to be implemented before in-bot paid VIP is enabled. No actual provider calls or refunds performed during development.

Operator commands (run only on the intended bot container; no provider charge/refund creation):

```sh
docker compose exec bot python -m scripts.reconcile_payments list
docker compose exec bot python -m scripts.reconcile_payments legacy PROVIDER_UUID
docker compose exec bot python -m scripts.reconcile_payments reconcile USER_ID ORDER_UUID PROVIDER_UUID
# Only after inspecting the preview and matching the merchant's records:
# docker compose exec bot python -m scripts.reconcile_payments reconcile USER_ID ORDER_UUID PROVIDER_UUID --apply
```

An unbound order with no provable provider ID remains blocked for manual merchant resolution; no guessed cancellation or re-creation. Refund observations are not a refund command, nor automatic entitlement revocation. These are manual reconciliation facilities, not automatic provider polling or a completed refund business policy.

### 10.2a — Stars implementation

Separate XTR tariffs and order/charge/refund ledger; 30-minute quote, private invoice, exact pre-checkout identity/amount/currency, one checkout per invoice, transactional idempotent VIP grant after successful_payment. Refund events record evidence, not automatic subtraction from mixed access. Previously authorized payments are processed even if sales are disabled. One-off 30 days per month, not recurring. No default invented price. Owner-only web tariff management and currency-separated payment export/history. TGTEACHER_STARS_ENABLED defaults to 0. Operator reconciliation for missed Telegram events, refund initiation and independent YooKassa checkout remain open. CI confirmation pending.

### 10.2a / 10.2b — operator visibility and SUP-01

Предыдущая Stars-регрессия: 117/117, GHA 45682996007. Добавлен read-only список локальных XTR-заказов с фильтрацией и пагинацией:
```sh
docker compose exec bot python -m scripts.reconcile_payments stars-list
docker compose exec bot python -m scripts.reconcile_payments stars-list --state all --limit 100 --offset 0
```
По умолчанию показываются authorized: это кандидаты для проверки, не доказательство оплаты. Команда не обращается к Telegram, не начисляет VIP и не инициирует возврат. Полноценная сверка пропущенных событий с Telegram и политика возвратов остаются блокерами.

SUP-01: legacy поддержка сначала доставляет вопрос в группу и атомарно сохраняет индекс, только затем удаляет сообщение и подтверждает получение. Ошибки конфигурации, отправки или записи оставляют исходный вопрос и флаг ожидания; учебные handlers не получают вопрос. Web-режим сохраняет прежний порядок. При неопределённом исходе Telegram или сбое записи после доставки повтор может создать дубликат; это не exactly-once. Тесты имитируют ошибки и проверяют сохранность индекса. Production не переключается.

### 10.2b — DATA-01: защита удалённых профилей от повторного импорта

Удаление и регистрация используют одну транзакционную блокировку ученика. Маркер удаления (Telegram ID и время, без имени/username) сохраняется даже после повторной регистрации: старые JSON/context-импорты больше не восстанавливают профиль и не перезаписывают свежие данные. Удалённый профиль может зарегистрироваться заново по явному /start в личном чате; при этом очищается остаточный учебный прогресс и локальный user_data. Обычный /start больше не создаёт дублирующую запись legacy JSON. Legacy helper продления VIP не создаёт удалённого ученика. Telegram-админ удаляет локальный контекст; web-админ использует ту же транзакцию удаления в БД.

Это защита от resurrection через фоновые импорты, не полное уничтожение персональных данных: старые JSON, резервные копии, записи журналов и финансовые ledger остаются по существующим правилам. Политика хранения/анонимизации и обработка активных in-flight учебных callbacks и восстановления старой копии БД требуют отдельной приёмки. Не обещается отзыв уже отправленных сообщений или аннулирование оплаченных заказов. Маркер необходим для предотвращения повторного импорта. Тесты проверяют конкурентное удаление/upsert, повторную регистрацию, невозможность старого импорта после неё и запрет повторного создания через legacy VIP.

### 10.2b — SEC-01: first container and credential isolation slice

Bot, MCP and web-admin runtime images use UID/GID 10001. Application code remains root-owned; only intended bind mounts must be writable. Compose drops Linux capabilities and enables no-new-privileges for the three application services. Web admin no longer inherits MCP bearer/OAuth credentials; its optional AI provider settings are explicit in admin.env.example. Removed the legacy tracked bot/admins.txt from the branch without displaying its contents; Git history and main are unchanged. Added CI checks for actual runtime UID/GID, effective capabilities, no-new-privileges, code write denial, mount permissions and service-specific secrets. Migration requires staging backup/ownership preparation and explicit provider configuration, as documented in RELEASE_RUNBOOK. CI pending. Remaining SEC-01: database least-privilege roles, dependency/history secret scans, rotation of any exposed credentials, and staging verification. No production changes.

### 10.2b — SEC-01: isolated PostgreSQL runtime roles

Added opt-in compose overlay and offline schema/role provisioning. Bot/MCP/admin run under distinct non-superuser DB credentials, without schema ownership, CREATE/TEMP or privileged role membership. Runtime startup verifies the schema release marker instead of executing DDL. Explicit per-table grants keep MCP away from finance, support and web identity; web admin can report Stars but not mutate Stars order states; bot cannot read web admin accounts. CI uses isolated overlay and real negative permission probes plus the existing browser/MCP regression. The base compose remains compatibility-only; staging migration, password generation and approved rollback are required before production. Grants intentionally preserve legitimate legacy admin and shared content/progress writes; no RLS or claim of exhaustive least privilege. CI pending.

### 10.2b — SEC-01 / BUILD-01: audit bootstrap

Dedicated read-only GitHub Actions workflow scans reachable Git history and current checkout with Gitleaks, with redacted output and metadata-only evidence. No blanket allowlist; any findings or scanner execution failures are non-green. The dependency job resolves candidate Python 3.12 hashed and npm locks on a real runner, validates hash-checked pip install, npm ci and web build, emits inventories and runs Python/npm advisory audits. Python findings and high/critical npm advisories block the audit. Initial lock candidates are CI artifacts, NOT yet committed or enforced in release Dockerfiles. Review actual reports, remediate findings and verify the locks before switching production builds to them. Secret rotation requires operator access; do not reproduce values in issues or chat. Image digest pinning and OS package/container vulnerability scan remain open.

### 10.2b — dependency advisory remediation

Initial Security and dependency audit run 45727546542: Gitleaks reported 0 findings in reachable fetched Git history and 0 in the current tree. Candidate lock resolution, hash-checked Python install, npm ci and web build succeeded. pip-audit reported 42 advisory records in Pillow 12.3.0, with fixes listed in 13.0.0. npm audit reported one affected package (Vite 7.2.2, high severity; multiple underlying advisory records), with a non-major recommended fix to 7.3.8. The two direct dependency versions are updated to Pillow 13.0.0 and Vite 7.3.8. No advisory is ignored and no security threshold is reduced. The scanner result is limited to reachable history and scanner coverage; it does not certify absence of secrets. Repeat the dedicated audit and all regression/smoke jobs; do not approve production or declare BUILD-01 closed before passing them and committing/enforcing the resulting lockfiles. Image digest pinning and OS/container scan remain open.


## BUILD-01: verified lock enforcement

Imported exact artifacts from successful audit 45728975647 after SHA-256 verification (import run 45730237097, commit 0bafaf6). Python and npm locks are committed; Docker and regression CI use hash-verified pip and npm ci without unlocked fallback. Python and Node base images are pinned to digests observed in release-smoke 45728975721. Regenerate locks deliberately on Python 3.12 / Node 22 and review the security audit before committing changes. PostgreSQL image digest, apt package reproducibility and OS/container CVE scan remain open. This is not bit-for-bit build reproducibility. Fresh CI confirmation and staging acceptance are required.


## BUILD-01: PostgreSQL digest and release image scanning

Pin PostgreSQL 16 in Compose and the regression service to the exact registry digest recorded during successful run 45730519558 (job 139611083839). The release-smoke job now scans the actual built bot, MCP, admin, one-shot database migrator and the pinned database image with versioned Trivy. It saves each local image to an archive and scans without mounting the Docker socket. JSON reports include package inventories, all severities and local image IDs; HIGH/CRITICAL findings (including unfixed findings), missing scan targets and scanner errors fail the job. No advisory allowlist or ignore-unfixed switch is applied. Scanner output and findings require review; lower severity findings are retained. This does not attest production or guarantee bitwise reproducibility: apt repositories, scanner database and other CI tooling evolve. Follow-up must review real results, patch base images or OS packages as appropriate, and promote the same tested image digests in staging rather than rebuild. No production changes.


## BUILD-01: first OS remediation

Trivy run 45731389231 found 322 HIGH / 10 CRITICAL package-advisory records per application image, 99 HIGH / 2 CRITICAL for PostgreSQL. These are not counts of distinct CVEs or verified reachable exploits. Most OS findings had no fixed version listed. The first remediation installs available Debian updates and asserts libzstd1 >= 1.5.7+dfsg-1+deb13u1 (CVE-2026-124851); the database derivative also updates zstd and retains the original pinned PostgreSQL 16 base, entrypoint and volume. Build db explicitly before starting or upgrading it (docker compose build db); existing regression service remains the upstream digest, while release smoke uses the patched derivative. The migration-only build target excludes FFmpeg and its OS dependencies; app services retain media functionality and gain a synthetic Opus encode/probe smoke check. Go runtime advisories in upstream PostgreSQL helper binaries and Debian advisories without available fixes remain under investigation. The HIGH/CRITICAL gate is unchanged, and a new report is necessary before claiming remediation success. No production deployment.


## BUILD-01: PostgreSQL gosu toolchain remediation

Artifact 15077632774 from run 45732730695 identifies the sole Go-binary target as /usr/local/bin/gosu (built with Go 1.24.6). Rebuild upstream gosu 1.20 from commit 29b939aa5bd4dc588af765b8248f6e39150d2565 using Go 1.27.4, which is listed in docker-library/official-images. Use standard Go module checksum validation; do not disable sumdb. Copy only the rebuilt executable into the pinned PostgreSQL derivative; verify runtime Go version, UID/GID switching, PostgreSQL PID 1 identity and readiness. The builder is pinned to an exact version tag, not yet a digest; record the resolved digest and validate the new Trivy report. This change targets 26 fixable Go advisory records, not all PostgreSQL vulnerabilities. FFmpeg and libxml2 remain open: Trivy reported no fixed Debian versions for their critical findings. The full HIGH/CRITICAL release gate remains strict, and no production deployment is approved.


## BUILD-01: reduce FFmpeg exposure in learner bot

Run 45734100866 confirmed gosu 1.20 built with Go 1.27.4 and removed all 26 Go HIGH/CRITICAL records. Pin the exact Go builder digest from that successful build. The latest scanner report listed no fixed version for any remaining HIGH/CRITICAL finding. Debian trackers for CVE-2026-135736 (FFmpeg deband filter) and CVE-2026-6653 (libxml2) still describe vulnerable packages in stable; see https://security-tracker.debian.org/tracker/CVE-2026-135736 and https://security-tracker.debian.org/tracker/CVE-2026-6653. The Telegram stage-7 playback path retrieves already encoded MP3 from DB or legacy file and sends it directly. Media generation/conversion is invoked by MCP and web admin, not learner playback. Compose bot now builds a dedicated non-FFmpeg target, while MCP and web admin retain conversion support and default standalone Docker build remains media-capable. Limit base build COPY to bot code instead of including the frontend source. CI checks absence of ffmpeg/ffprobe in bot and migrator, real PCM-to-MP3 conversion in MCP/admin, and the stored-audio read path in bot with a synthetic mocked connection. No provider calls or Telegram sends. The known FFmpeg deband advisory is not a feature used by the existing fixed audio-only conversion command, but the remaining installed FFmpeg and libxml2 advisories are not declared fixed or accepted. Do not remove PostgreSQL-linked libraries or mix Debian release packages blindly. The strict HIGH/CRITICAL release gate remains active. Production remains NO-GO pending vulnerability disposition and staging validation.
