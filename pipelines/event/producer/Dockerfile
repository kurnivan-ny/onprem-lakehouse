FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pipelines/event/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipelines/event/producer/producer.py .
CMD ["python", "producer.py"]
