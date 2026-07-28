FROM python:3.12-slim

WORKDIR /app

COPY de/requirements.txt de/requirements.txt
RUN pip install --no-cache-dir -r de/requirements.txt

COPY contracts/VERSION contracts/VERSION
COPY de/ de/

ENV PYTHONPATH=/app
ENV DE_HOST=0.0.0.0
ENV DE_PORT=8080

EXPOSE 8080

CMD ["uvicorn", "de.webhook.main:app", "--host", "0.0.0.0", "--port", "8080"]
