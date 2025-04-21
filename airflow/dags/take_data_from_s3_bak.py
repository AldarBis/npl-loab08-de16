'''
import boto3

# Конфигурация
endpoint_url = 'https://storage.yandexcloud.net'
bucket_name = 'npl-de16-lab8-data'
object_key = 'year=2025/month=04/day=13/hour=14/device_events.jsonl.zip'
download_path = '/root/airflow/airflow_home/raw_data/device.jsonl.zip'

# Создание клиента
s3 = boto3.client(
    's3',
    endpoint_url=endpoint_url,
)

# Загрузка файла
s3.download_file(bucket_name, object_key, download_path)
print(f"Файл скачан в {download_path}")
'''

import boto3
import os

# Конфигурация
endpoint_url = 'https://storage.yandexcloud.net'
bucket_name = 'npl-de16-lab8-data'
download_dir_loc = './downloads/location_events'
download_dir_brow = './downloads/browser_events'
download_dir_device = './downloads/device_events'
download_dir_geo = './downloads/geo_events'

# Создаём директорию, если не существует
os.makedirs(download_dir_loc, exist_ok=True)
os.makedirs(download_dir_brow, exist_ok=True)
os.makedirs(download_dir_device, exist_ok=True)
os.makedirs(download_dir_geo, exist_ok=True)
# Клиент S3
s3 = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=os.environ.get("YANDEX_KEY"),
    aws_secret_access_key=os.environ.get("YANDEX_SECRET")
)

# Пагинация по объектам
paginator = s3.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name)

# Проход по всем страницам
for page in page_iterator:
    for obj in page.get('Contents', []):
        key = obj['Key']

        # Фильтруем только нужные файлы
        if key.endswith('location_events.jsonl.zip'):
            filename = key.replace('/', '__')  # Плоское имя файла
            local_path = os.path.join(download_dir_loc, filename)
            print(f'  Скачиваем: {key}')
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f'✅ Сохранено: {local_path}')
            except Exception as e:
                print(f'❌ Ошибка при скачивании {key}: {e}')

        if key.endswith('browser_events.jsonl.zip'):
            filename = key.replace('/', '__')  # Плоское имя файла
            local_path = os.path.join(download_dir_brow, filename)
            print(f'  Скачиваем: {key}')
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f'✅ Сохранено: {local_path}')
            except Exception as e:
                print(f'❌ Ошибка при скачивании {key}: {e}')

        if key.endswith('device_events.jsonl.zip'):
            filename = key.replace('/', '__')  # Плоское имя файла
            local_path = os.path.join(download_dir_device, filename)
            print(f'  Скачиваем: {key}')
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f'✅ Сохранено: {local_path}')
            except Exception as e:
                print(f'❌ Ошибка при скачивании {key}: {e}')

        if key.endswith('geo_events.jsonl.zip'):
            filename = key.replace('/', '__')  # Плоское имя файла
            local_path = os.path.join(download_dir_geo, filename)
            print(f'⬇️ Скачиваем: {key}')
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f'✅ Сохранено: {local_path}')
            except Exception as e:
                print(f'❌ Ошибка при скачивании {key}: {e}')

