FROM openjdk:17-alpine

WORKDIR /job
COPY . .

RUN apk add --no-cache coreutils
RUN javac -d . "{{ENTRY_POINT_PATH}}"

CMD ["stdbuf", "-oL", "-eL", "java", "{{ENTRY_POINT_FILENAME}}"]
