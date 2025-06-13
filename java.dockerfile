FROM openjdk:17-alpine

WORKDIR /job
COPY . .

RUN javac $(find . -name "*.java")

CMD ["java", "-Djava.util.logging.ConsoleHandler.level=ALL", "-Dfile.encoding=UTF8", "-Xshare:off", "{{ENTRY_POINT_FILENAME}}"]
