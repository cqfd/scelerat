FROM python:3.11.1-slim-buster 
WORKDIR /http_worker
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY http_worker.py .
CMD ["celery", "-A", "http_worker", "worker", "--loglevel=INFO", "-Q", "http", "--pool", "gevent", "--concurrency", "100", "--without-mingle"]