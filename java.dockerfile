FROM openjdk:17
WORKDIR /job
COPY . .
RUN javac "{{ENTRY_POINT_PATH}}"
CMD ["java", "{{ENTRY_POINT_FILENAME}}"]
