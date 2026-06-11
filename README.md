# OpenFoodFacts KMeans Clustering on PySpark

Кластеризация продуктов из OpenFoodFacts с помощью PySpark и алгоритма K-средних.

## Установка

```bash
pip install -r requirements.txt
```

Данные положить в `data/food.parquet` (или указать свой путь в `config.yaml`).

## WordCount

Проверка работоспособности Spark:

```bash
python test/wordcount.py
```

## Кластеризация

```bash
python src/main.py --config config.yaml
```

Гиперпараметры задаются в `config.yaml`.

## Источник данных: MS SQL Server

Модель и источник данных взаимодействуют через MS SQL Server.

- Формат хранения: `food_input` (числовые признаки), `food_predictions` (признаки + номер кластера), `food_metrics` (`metric`, `value`).
- Выгрузка при каждом запуске: на старте модель пишет исходные данные в `food_input` и читает их обратно из SQL Server.
- Загрузка по завершении: сразу после обучения предсказания и метрики записываются в `food_predictions` и `food_metrics`.

Параметры подключения — в секции `sqlserver` файла `config.yaml`; пароль и хост
передаются через переменные окружения `MSSQL_PASSWORD` и `MSSQL_HOST`.

## Запуск в Docker

```bash
docker compose up
```

Поднимается контейнер MS SQL Server и контейнер модели, который запускается после готовности БД.
