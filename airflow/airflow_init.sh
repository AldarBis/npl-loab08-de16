#!/bin/bash

echo "⏳ Остановка и очистка контейнеров..."
docker-compose down -v

echo "🚧 Инициализация базы данных Airflow..."
docker-compose run --rm airflow airflow db init

echo "👤 Создание пользователя admin..."
docker-compose run --rm airflow \
  airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

echo "🚀 Запуск всех сервисов..."
docker-compose up

