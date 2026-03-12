FROM python:3.14-alpine

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./
COPY templates ./templates
COPY static ./static

ENV FLASK_RUN_HOST=0.0.0.0 \
    FLASK_RUN_PORT=8080

EXPOSE 8080

CMD ["python", "app.py"]
