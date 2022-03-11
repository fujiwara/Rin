FROM golang:alpine3.15

WORKDIR /build_dir

COPY go.mod ./
COPY go.sum ./

COPY redshift.go ./
COPY event.go ./
COPY rin.go ./
COPY main.go ./
COPY config.go ./

RUN go get

RUN go build -o /build_dir/ main.go rin.go config.go event.go redshift.go


FROM alpine:3.12.4
LABEL maintainer "fujiwara <fujiwara.shunichiro@gmail.com>"
RUN apk --no-cache add ca-certificates

COPY --from=0 /build_dir/main /usr/local/bin/rin

WORKDIR /
ENTRYPOINT ["/usr/local/bin/rin"]
