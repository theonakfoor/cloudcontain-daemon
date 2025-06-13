FROM openjdk:17-alpine

WORKDIR /job
COPY . .

RUN apk add --no-cache bash
RUN javac $(find . -name "*.java")

CMD ["bash", "-c", "java -Djava.util.logging.ConsoleHandler.level=ALL -Dfile.encoding=UTF8 -Xshare:off {{ENTRY_POINT_FILENAME}} 2> >(while read line; do echo \"[STDERR] $line\"; done) | while read line; do echo \"[STDOUT] $line\"; done"]