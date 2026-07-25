# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY docker/dashboard/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=180 --retries 10 -r requirements.txt

COPY dashboard ./dashboard

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0"]
