# Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV DASHBOARD_DB=/app/dashboard.db
ENV FRONTEND_DIR=/app/frontend

EXPOSE 8000

CMD ["uvicorn", "api.dashboard_server:app", "--host", "0.0.0.0", "--port", "8000"]
