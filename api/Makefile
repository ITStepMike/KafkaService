LIBRDPATH = /librd

#global command
.PHONY: all
all: dependencies kafkaAlias
	go build -o main .

.PHONY: dependencies
dependencies: 	
	go get github.com/Masterminds/glide
	glide install

.PHONY: kafkaAlias
kafkaAlias:
	@echo "executing kafka alias"
	@echo "127.0.0.1 localhost kafka" >> /etc/hosts
