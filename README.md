# OpenFoodFacts KMeans Clustering on PySpark

Кластеризация продуктов из OpenFoodFacts с помощью PySpark и алгоритма K-средних, развёрнутая в Kubernetes (на базе minikube).

## Данные

Полный `data/food.parquet` слишком велик для minikube, поэтому используется сэмпл
`data/food_small.parquet` (20к строк). Создать его:

```bash
python scripts/make_sample.py data/food.parquet data/food_small.parquet 20000
```

## Запуск в Kubernetes

```bash
bash scripts/run-k8s-lab.sh
```

Скрипт стартует minikube, собирает образы `lab8-app` и `lab8-datamart`, накатывает манифесты из `k8s/` и прогоняет обучение.

## Проверка

```bash
kubectl get po -n lab8
kubectl logs -n lab8 -l app=spark-train --tail=50

MSSQL_POD="$(kubectl get pod -n lab8 -l app=mssql -o jsonpath='{.items[0].metadata.name}')"
kubectl exec -n lab8 "$MSSQL_POD" -- /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'QwErTy123@!' -No -Q \
  "USE food; SELECT COUNT(*) FROM food_predictions; SELECT metric, value FROM food_metrics;"
```

## Утилизация ресурсов

Лимиты в манифестах подобраны так, чтобы утилизация была 70–80%
(замерено на сэмпле 20к строк):

| Сервис | Ресурс | Пик / лимит | Утилизация |
|---|---|---|---|
| Модель | CPU | 1501m / 2 | 75% |
| Модель | RAM | 664Mi / 896Mi | 74% |
| Витрина | RAM | 1540Mi / 2Gi | 75% |

У `mssql` лимиты по CPU сложно подобрать, они при такой нагрузке 10–20%. Лимиты: модель —
`k8s/spark-job.yaml`, mssql — `k8s/mssql.yaml`.

Смотрел через `metrics-server`:

```bash
minikube addons enable metrics-server
kubectl rollout status deploy/metrics-server -n kube-system
```

Текущее потребление против лимитов (утилизация = `usage / limit`):

```bash
kubectl top pods -n lab8
```

Пик RAM модели:

```bash
POD=$(kubectl get pod -n lab8 -l app=spark-train -o jsonpath='{.items[0].metadata.name}')
while kubectl get pod -n lab8 "$POD" -o jsonpath='{.status.phase}' | grep -q Running; do
  kubectl exec -n lab8 "$POD" -- cat /sys/fs/cgroup/memory.peak
  sleep 3
done
```

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
