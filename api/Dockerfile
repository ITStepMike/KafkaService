FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

RUN apk add --update --no-cache alpine-sdk bash ca-certificates \
      libressl \
      tar \
      git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils

COPY ./ /go/src/github.com/KafkaService/api
WORKDIR /go/src/github.com/KafkaService/api

RUN make