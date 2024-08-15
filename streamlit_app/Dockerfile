# streamlit_app/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Скопировать файл с зависимостями и установить их
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Скопировать исходный код приложения
COPY . .

# Убедиться, что директория для данных существует
RUN mkdir -p /app/data

# Команда для запуска Streamlit приложения
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
