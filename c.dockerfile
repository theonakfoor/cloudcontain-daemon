FROM alpine:latest

RUN apk add --no-cache gcc libc-dev make coreutils bash

WORKDIR /job
COPY . .

RUN gcc $(find . -name "*.c") -o main

CMD ["./main"]