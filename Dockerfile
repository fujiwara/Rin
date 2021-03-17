FROM alpine:3.12.4
LABEL maintainer "fujiwara <fujiwara.shunichiro@gmail.com>"

ARG VERSION=1.1.0-pre
RUN apk --no-cache add ca-certificates curl
RUN curl -sL https://github.com/fujiwara/Rin/releases/download/v${VERSION}/Rin_${VERSION}_linux_amd64.tar.gz \
    | tar zxvf - \
    && install rin /usr/local/bin
WORKDIR /
ENTRYPOINT ["/usr/local/bin/rin"]
