BUF ?= npx @bufbuild/buf
OUT_DIR ?= bin/

.PHONY: all build clean format proto test

all: build

build:
	mkdir -p ${OUT_DIR}
	go build -o ${OUT_DIR} ./...

test:
	go test ./...

clean:
	rm -rf ${OUT_DIR}

format:
	go fmt ./...
	go mod tidy

proto:
	rm -rf ./api/gen/
	${BUF} generate