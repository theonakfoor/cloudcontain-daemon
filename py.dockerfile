FROM python:3.12-slim

WORKDIR /job
COPY . .

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y bash

CMD ["bash", "-c", "python3 -u {{ENTRY_POINT_FILE}} 2> >(while read line; do echo \"[STDERR] $line\"; done) | while read line; do echo \"[STDOUT] $line\"; done"]
