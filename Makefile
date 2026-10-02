BASE_URL ?= http://localhost:8080
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/seats?sslmode=disable

.PHONY: up down logs build run test burst

up:            ## run app + postgres exactly as deployed
	docker compose up --build -d
	@echo "waiting for readiness..."; until curl -fs $(BASE_URL)/readyz >/dev/null; do sleep 1; done; echo ready

down:
	docker compose down -v

logs:
	docker compose logs -f app

build:
	mvn -B -q -DskipTests package

run: build     ## run against a local postgres
	java -jar target/seat-reservation.jar

test:          ## integration tests (need a Postgres at TEST_DATABASE_URL)
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" mvn -B test

burst:         ## make burst BASE_URL=https://your-app
	./burst.sh $(BASE_URL)
