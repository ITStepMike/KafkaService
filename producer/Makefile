LIBRDPATH = /librd

#global command
.PHONY: all
all: dependencies
	go build -o main .

.PHONY: dependencies
dependencies: librdkafka		
	go get github.com/Masterminds/glide
	glide install

librdkafka:
	mkdir $(LIBRDPATH); \
		cd $(LIBRDPATH); \
		git clone https://github.com/edenhill/librdkafka.git; \
		cd $(LIBRDPATH)/librdkafka; \
		$(LIBRDPATH)/librdkafka/configure; \
		make; \
		make install; \
		mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2 \
