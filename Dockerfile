FROM alpine:3.15.0
LABEL maintainer "fujiwara <fujiwara.shunichiro@gmail.com>"

RUN apk --no-cache add ca-certificates
COPY dist/Rin_linux_amd64_v1/rin /usr/local/bin/rin
WORKDIR /
ENTRYPOINT ["/usr/local/bin/rin"]
