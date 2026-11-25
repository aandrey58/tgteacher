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
