# airflow-sandbox

Запуск окружения:

docker compose up --build

В случае ошибки "ERROR: You need to initialize the database. Please run `airflow db init`. Make sure the command is run using Airflow version 2.5.1." выполнить airflow db init вручную:

```
docker compose down
sudo docker compose run --rm airflow-webserver airflow db migrate
docker compose up
```

Добавление пользователя для входа:
1. Заходим в контейнер: docker exec -it airflow-webserver-1 bash
2. Выполняем команду:

``` 
airflow users create \
       --role Admin \
       --username admin \
       --email admin@example.com \
       --firstname Admin \
       --lastname User \
       --password new_secure_password
```

Либо сразу:

```
sudo docker compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

Шпаргалка

Рестарт сервисов:

```
docker compose restart webserver
docker compose restart scheduler
```

Отключение тестовых примеров: 
AIRFLOW__CORE__LOAD_EXAMPLES=False

Отображение конфига в Airflow UI
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True