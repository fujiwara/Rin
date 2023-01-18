GIT_VER := $(shell git describe --tags)
DATE := $(shell date +%Y-%m-%dT%H:%M:%S%z)
export GO111MODULE := on

.PHONY: test test-localstack install clean image release-image

cmd/rin/rin: *.go cmd/rin/main.go
	cd cmd/rin && go build -ldflags "-s -w -X main.version=${GIT_VER} -X main.buildDate=${DATE}"

install: cmd/rin/rin
	install cmd/rin/rin ${GOPATH}/bin

test-localstack:
	docker-compose up -d
	TEST_LOCALSTACK=on dockerize -timeout 30s -wait tcp://localhost:4566 go test -v -run Local ./...

test:
	go test -v ./...

dist/:
	goreleaser build --snapshot --rm-dist

clean:
	rm -rf cmd/rin/rin pkg/* test/ls_tmp/* dist/

image: dist/
	docker build \
		--tag ghcr.io/fujiwara/rin:$(GIT_VER) \
		.

release-image: image
	docker push ghcr.io/fujiwara/rin:$(GIT_VER)
