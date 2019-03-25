FROM golang:1.12.1-stretch AS build-env

ENV CGO_ENABLED=0
RUN mkdir -p /go/src/github.com/fujiwara/Rin
COPY . /go/src/github.com/fujiwara/Rin
WORKDIR /go/src/github.com/fujiwara/Rin
RUN make clean
RUN make test
RUN make install

FROM alpine:3.9
LABEL maintainer "fujiwara <fujiwara.shunichiro@gmail.com>"

RUN apk --no-cache add ca-certificates
COPY --from=build-env /go/bin/rin /usr/local/bin
WORKDIR /
ENTRYPOINT ["/usr/local/bin/rin"]
