FROM python:latest
# COPY ./src /app/src

WORKDIR /app
COPY . /app
RUN pip install -e .

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# ONLY USE FOR WHEN WRITING TO /srv/data/ location
RUN addgroup --gid 1001 appusergroup
RUN adduser -u 1001 --gid 1001 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser
# ENTRYPOINT /bin/bash