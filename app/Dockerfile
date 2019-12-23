FROM golang:alpine AS builder

COPY ./ /go/src/github.com/KafkaService/app
WORKDIR /go/src/github.com/KafkaService/app

RUN apk update && apk add --no-cache git

RUN go get -d -v

RUN go build
