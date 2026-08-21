.PHONY: all test run lint clean docker-build

all: test

test:
	go test ./...

run:
	go run cmd/server/main.go

lint:
	@echo "Running lint checks..."

clean:
	@echo "Cleaning artifacts..."

docker-build:
	docker build -t app:latest .
