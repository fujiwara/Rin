GIT_VER := $(shell git describe --tags)
DATE := $(shell date +%Y-%m-%dT%H:%M:%S%z)
export GO111MODULE := on

.PHONY: test local get-deps install clean

cmd/rin/rin: config.go redshift.go rin.go event.go cmd/rin/main.go
	cd cmd/rin && go build -ldflags "-s -w -X main.version=${GIT_VER} -X main.buildDate=${DATE}"

install: cmd/rin/rin
	install cmd/rin/rin ${GOPATH}/bin

test-localstack:
	docker-compose up -d
	TEST_LOCALSTACK=on dockerize -timeout 30s -wait tcp://localhost:4576 go test -v -run Local ./...

test:
	go test -v ./...

packages: config.go redshift.go rin.go event.go
	cd cmd/rin && gox -os="linux darwin" -arch="amd64" -output "../../pkg/{{.Dir}}-${GIT_VER}-{{.OS}}-{{.Arch}}" -ldflags "-s -w -X main.version=${GIT_VER} -X main.buildDate=${DATE}"
	cd pkg && find . -name "*${GIT_VER}*" -type f -exec zip {}.zip {} \;

clean:
	rm -f cmd/rin/rin pkg/* test/ls_tmp/*

image:
	docker build \
		--build-arg VERSION=$(GIT_VER) \
		--tag ghcr.io/fujiwara/rin:$(GIT_VER) \
		.

release-image: image
	docker push ghcr.io/fujiwara/rin/$(GIT_VER)
