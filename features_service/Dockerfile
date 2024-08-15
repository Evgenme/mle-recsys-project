# features_service/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Скопировать файл с зависимостями и установить их
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Скопировать исходный код приложения
COPY . .

# Убедиться, что директория для данных существует
RUN mkdir -p /app/data

# Команда для запуска FastAPI приложения
CMD ["uvicorn", "features_service:app", "--host", "0.0.0.0", "--port", "8010"]
