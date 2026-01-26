# airflow-sandbox

Запуск окружения:

docker-compose up --build

В случае ошибки "ERROR: You need to initialize the database. Please run `airflow db init`. Make sure the command is run using Airflow version 2.5.1." выполнить airflow db init вручную:

docker-compose down
docker-compose run --rm webserver airflow db init
docker-compose up

Добавление пользователя для входа:
1. Заходим в контейнер: docker exec -it airflow-webserver-1 bash
2. Выполняем команду:

airflow users create \
       --role Admin \
       --username admin \
       --email admin@example.com \
       --firstname Admin \
       --lastname User \
       --password new_secure_password
