LIBRDPATH = /librd

#global command
.PHONY: all
all: dependencies
	go build -o main .

.PHONY: dependencies
dependencies: 		
	go get github.com/Masterminds/glide
	glide install
