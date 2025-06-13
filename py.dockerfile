FROM python:3.12-slim

WORKDIR /job
COPY . .

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y coreutils

CMD ["stdbuf", "-oL", "-eL", "python3", "{{ENTRY_POINT_FILE}}"]