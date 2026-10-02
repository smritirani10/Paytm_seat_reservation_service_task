BASE_URL ?= http://localhost:8080
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/seats?sslmode=disable

.PHONY: up down logs build test burst

up:            ## run app + postgres exactly as deployed
	docker compose up --build -d
	@echo "waiting for readiness..."; until curl -fs $(BASE_URL)/readyz >/dev/null; do sleep 1; done; echo ready

down:
	docker compose down -v

logs:
	docker compose logs -f app

build:
	go build -o bin/server ./cmd/server && go build -o bin/burst ./cmd/burst

test:          ## integration tests (need a Postgres at TEST_DATABASE_URL)
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 ./...

burst:         ## make burst BASE_URL=https://your-app
	./burst.sh $(BASE_URL)
