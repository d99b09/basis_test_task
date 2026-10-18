FROM dockerhub.timeweb.cloud/library/python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY server.py ./server.py

EXPOSE 8080

CMD ["python", "server.py"]
