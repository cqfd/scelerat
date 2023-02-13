FROM python:3.11.1-slim-buster 
WORKDIR /worker
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY worker.py .
CMD ["celery", "-A", "worker", "worker", "--loglevel=INFO", "-Q", "continuation"]