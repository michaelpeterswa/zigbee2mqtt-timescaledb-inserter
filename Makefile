# Makefile
all: setup hooks

# requires `nvm use --lts` or `nvm use node`
.PHONY: setup
setup: 
	npm install -g @commitlint/config-conventional @commitlint/cli  


.PHONY: hooks
hooks:
	@git config --local core.hooksPath .githooks/
# Local TimescaleDB for the integration tests, with the sensors schema from a
# checkout of lfprocks/timescale-migrations applied:
#   make testdb MIGRATIONS_DIR=../../lfprocks/timescale-migrations/migrations
#   TIMESCALE_TEST_CONN_STRING=$$(make -s testdb-url) go test ./...
TESTDB_CONTAINER ?= zigbee2mqtt-inserter-testdb
TESTDB_PORT ?= 55432
MIGRATIONS_DIR ?= ../timescale-migrations/migrations

.PHONY: testdb
testdb:
	@docker rm --force $(TESTDB_CONTAINER) >/dev/null 2>&1 || true
	@docker run --detach --name $(TESTDB_CONTAINER) --publish 127.0.0.1:$(TESTDB_PORT):5432 \
		--env POSTGRES_PASSWORD=example timescale/timescaledb:2.17.2-pg16 >/dev/null
	@until docker exec $(TESTDB_CONTAINER) pg_isready --username=postgres --quiet; do sleep 1; done
	@sleep 2
	@docker run --rm --network host --volume "$(abspath $(MIGRATIONS_DIR))":/migrations:ro migrate/migrate:4 \
		-path /migrations -database "postgres://postgres:example@127.0.0.1:$(TESTDB_PORT)/postgres?sslmode=disable" up

.PHONY: testdb-url
testdb-url:
	@echo "postgres://postgres:example@127.0.0.1:$(TESTDB_PORT)/postgres?sslmode=disable"

.PHONY: testdb-stop
testdb-stop:
	@docker rm --force $(TESTDB_CONTAINER) >/dev/null 2>&1 || true
