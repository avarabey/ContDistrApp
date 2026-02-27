# RefData Platform

Реализация backend-платформы справочников по `Epic.md`.

## Что реализовано

- REST command API:
  - `POST /v1/tenants/{tenantId}/updates?consistencyMode=ASYNC|WAIT_COMMIT`
  - `GET /v1/tenants/{tenantId}/updates/{eventId}`
- REST query API:
  - `GET /v1/tenants/{tenantId}/dictionaries/{dictCode}/items/{key}`
  - `GET /v1/tenants/{tenantId}/dictionaries/{dictCode}/items?keys=...`
  - `GET /v1/tenants/{tenantId}/dictionaries/{dictCode}/all`
  - `GET /v1/tenants/{tenantId}/dictionaries/{dictCode}/version`
- Metadata-driven `DictionaryProvider` с SQL-конфигом словарей.
- Pipeline применения изменений:
  - идемпотентность через `processed_event`
  - `DELTA` и `SNAPSHOT` (включая chunked snapshot)
  - версия в `dictionary_meta.version`
  - outbox + relay в invalidation bus
  - транспорт команд: in-memory или Kafka (`refdata.kafka.enabled`)
  - внешний Kafka-адаптер (`refdata.kafka.external-enabled`, topic `refdata.kafka.external-topic`)
- Query кэш Pod:
  - in-memory snapshot + atomic swap
  - single-flight reload
  - `X-Min-Version` барьер + fallback в PostgreSQL
  - транспорт инвалидаций: in-memory или Redis Pub/Sub + Stream (`refdata.redis.enabled`)
- Tenant guard по заголовку `X-Auth-Tenant` (для тестового auth-контекста).
- Контракты:
  - `docs/openapi.yaml`
  - `docs/schemas/*.json`

## Структура

- `refdata-shared` — общий домен, persistence, сервисная логика, миграции, тесты
- `command-api-app` — отдельное приложение `command-api` (роль `command-api`)
- `apply-service-app` — отдельное приложение `apply-service` (роль `apply-service`)
- `query-api-app` — отдельное приложение `query-api` (роль `query-api`)
- `outbox-relay-app` — отдельное приложение `outbox-relay` (роль `outbox-relay`)

## Запуск

```bash
mvn test
```

Запуск отдельных сервисов:

```bash
mvn -pl command-api-app -am spring-boot:run
mvn -pl apply-service-app -am spring-boot:run
mvn -pl query-api-app -am spring-boot:run
mvn -pl outbox-relay-app -am spring-boot:run
```

`-am` обязателен при запуске из корня multi-module проекта: он собирает зависимый модуль
`refdata-shared`, иначе возникает ошибка `Could not find artifact com.contdistrapp:refdata-shared`.

По умолчанию используется in-memory H2 в PostgreSQL-режиме (в shared-конфигурации).

### Включение Kafka

```yaml
refdata:
  kafka:
    enabled: true
    external-enabled: false
    commands-topic: refdata.commands
    external-topic: refdata.external.commands
    group-id: refdata-apply-service
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

### Включение Redis для инвалидаций

```yaml
refdata:
  redis:
    enabled: true
    pub-channel: refdata:inv:pub
    stream-key: refdata:inv:stream
spring:
  data:
    redis:
      host: localhost
      port: 6379
```

## Тесты

```bash
mvn test
```

## VS Code

- JDK и Maven установлены через Homebrew:
  - `openjdk 25.0.2`
  - `maven 3.9.12`
- В проект добавлены:
  - `.vscode/settings.json` (пути Java/Maven)
  - `.vscode/extensions.json` (рекомендации Java Pack + Maven)
