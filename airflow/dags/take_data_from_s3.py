from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
import shutil
import clickhouse_connect
import zipfile
import json

# 📦 Переменные окружения
endpoint_url = os.environ.get("S3_ENDPOINT_URL")
bucket_name = os.environ.get("YANDEX_BUCKET_NAME")

# 📁 Локальные папки загрузки
download_dir_loc = '/opt/airflow/downloads/location'
download_dir_brow = '/opt/airflow/downloads/browser'
download_dir_device = '/opt/airflow/downloads/device'
download_dir_geo = '/opt/airflow/downloads/geo'


# 🧹 Шаг 1: очистка папок
def clean_download_dirs():
    for path in [download_dir_loc, download_dir_brow, download_dir_device, download_dir_geo]:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)
    print("🧼 Папки очищены")


# 📥 Шаг 2: загрузка файлов из S3
def download_filtered_files_from_s3():
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("YANDEX_KEY"),
        aws_secret_access_key=os.environ.get("YANDEX_SECRET")
    )

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)

    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            filename = key.replace('/', '__')

            if key.endswith('location_events.jsonl.zip'):
                local_path = os.path.join(download_dir_loc, filename)
            elif key.endswith('browser_events.jsonl.zip'):
                local_path = os.path.join(download_dir_brow, filename)
            elif key.endswith('device_events.jsonl.zip'):
                local_path = os.path.join(download_dir_device, filename)
            elif key.endswith('geo_events.jsonl.zip'):
                local_path = os.path.join(download_dir_geo, filename)
            else:
                continue

            print(f'⬇️ Скачиваем: {key}')
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f'✅ Сохранено: {local_path}')
            except Exception as e:
                print(f'❌ Ошибка при скачивании {key}: {e}')

# 🧩 Шаг 3: Загрузка данных в ClickHouse
def upload_to_clickhouse():
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST"),
        port=int(os.environ.get("CLICKHOUSE_PORT")),
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database=os.environ.get("CLICKHOUSE_DB"),
    )
    datasets = {
        'location': '/opt/airflow/downloads/location',
        'browser': '/opt/airflow/downloads/browser',
        'device': '/opt/airflow/downloads/device',
        'geo': '/opt/airflow/downloads/geo',
    }

    def infer_type(value):
        """ Простое определение типа данных """
        if isinstance(value, bool):
            return "UInt8"
        elif isinstance(value, int):
            return "Int64"
        elif isinstance(value, float):
            return "Float64"
        elif isinstance(value, str):
            # возможно ISO8601 формат → DateTime
            try:
                from dateutil.parser import parse
                parse(value)
                return "DateTime"
            except:
                return "String"
        else:
            return "String"

    for table, directory in datasets.items():
        for filename in os.listdir(directory):
            if not filename.endswith(".jsonl.zip"):
                continue

            full_path = os.path.join(directory, filename)
            print(f"📦 Распаковка архива: {full_path}")

            with zipfile.ZipFile(full_path, 'r') as zf:
                for name in zf.namelist():
                    with zf.open(name) as f:
                        rows = []
                        for line in f:
                            row = json.loads(line.decode('utf-8'))
                            rows.append(row)

                        if not rows:
                            continue

                        # 🔍 Автоопределение схемы
                        first_row = rows[0]
                        columns = first_row.keys()
                        types = {k: infer_type(v) for k, v in first_row.items()}

                        # 🔧 Сборка SQL
                        col_defs = ",\n    ".join([f"{col} {types[col]}" for col in columns])
                        create_sql = f"""
                            CREATE TABLE IF NOT EXISTS ecommerce.{table}_events (
                                {col_defs}
                            ) ENGINE = MergeTree()
                            ORDER BY tuple()
                        """
                        print(f"🛠  Создание таблицы `{table}_events`, если не существует...")
                        client.command(create_sql)

                        cleaned_rows = []
                        skipped = []

                        for i, row in enumerate(rows):
                            if isinstance(row, dict):
                                try:
                                    filtered = {col: row[col] for col in columns}
                                    cleaned_rows.append(filtered)
                                except KeyError as e:
                                    print(f"⚠️ Пропущена строка {i}: нет ключа {e}")
                                    print(f"    Строка: {json.dumps(row, ensure_ascii=False)}")
                                    skipped.append((i, row))
                            else:
                                print(f"⚠️ Пропущена не-dict строка {i}: {row}")
                                skipped.append((i, row))

                        # Преобразуем в список списков в точном порядке колонок
                        prepared_data = [[row.get(col) for col in columns] for row in cleaned_rows]

                        client.insert(f"ecommerce.{table}_events", prepared_data, column_names=columns)


                        '''
                        print(f"⬆️ Загружается {len(rows)} строк в `{table}_events`")
                        client.insert(f"{table}_events", rows, column_names=columns)
                        '''


# 🎯 DAG
with DAG(
    dag_id='s3_download_and_cleanup',
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['s3', 'clean', 'download'],
    description='Очистка и скачивание файлов из S3',
) as dag:

    clear_downloads = PythonOperator(
        task_id='clear_downloads_folder',
        python_callable=clean_download_dirs
    )

    download_files = PythonOperator(
        task_id='download_filtered_jsonl_archives',
        python_callable=download_filtered_files_from_s3
    )

    upload_task = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse
    )

    clear_downloads >> download_files >> upload_task

