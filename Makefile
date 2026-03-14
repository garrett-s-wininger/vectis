BUF ?= npx @bufbuild/buf
COMPONENTS := api-server queue registry worker
OUT_DIR ?= bin/

.PHONY: all build clean $(COMPONENTS) format image-full image-% proto test

all: build

$(COMPONENTS):
	mkdir -p ${OUT_DIR}
	go build -o ${OUT_DIR}$@ ./cmd/$@

build:
	mkdir -p ${OUT_DIR}
	go build -o ${OUT_DIR} ./...

clean:
	rm -rf ${OUT_DIR}

format:
	go fmt ./...
	go mod tidy

image-full:
	podman build -t vectis:latest -f build/Containerfile --target all-in-one .

image-%:
	podman build -t vectis-${*}:latest -f build/Containerfile --target ${*} .

proto:
	rm -rf ./api/gen/
	${BUF} generate

test:
	go test -race ./...
