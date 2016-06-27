GIT_VER := $(shell git describe --tags)
DATE := $(shell date +%Y-%m-%dT%H:%M:%S%z)

.PHONY: test local get-deps install clean

cmd/rin/rin: config.go redshift.go rin.go event.go cmd/rin/main.go
	cd cmd/rin && go build -ldflags "-X main.version=${GIT_VER} -X main.buildDate=${DATE}"

install: cmd/rin/rin
	install cmd/rin/rin ${GOPATH}/bin

test:
	go test

get-deps:
	go get -t -d -v .
	cd cmd/rin && go get -t -d -v .

packages: config.go redshift.go rin.go event.go
	cd cmd/rin && gox -os="linux darwin" -arch="amd64" -output "../../pkg/{{.Dir}}-${GIT_VER}-{{.OS}}-{{.Arch}}" -ldflags "-X main.version=${GIT_VER} -X main.buildDate=${DATE}"
	cd pkg && find . -name "*${GIT_VER}*" -type f -exec zip {}.zip {} \;

clean:
	rm -f cmd/rin/rin
	rm -f pkg/*
