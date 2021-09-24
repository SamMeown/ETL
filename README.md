## Проект «Панель администратора» для кинотеатра

Админка создана на **Django** и позволяет легко и удобно создавать и редактировать сущности и связи в базе данных кинотеатра.  
В рамках проекта также реализован небольшой API, возвращающий список фильмов в формате, описанном в [openapi-файле 💾](/files/django_openapi.yml), и позволяющий получить информацию об одном и обо всех фильмах с пагинацией.

## Используемые технологии

- Приложение создано на **Django**
- В качестве базы данных используется **PostgreSQL**
- Приложение запускается под управлением сервера WSGI **Gunicorn**.
- Для отдачи статических файлов используется **Nginx.**
- Виртуализация осуществляется в **Docker**, взаимодействие между контейнерами через **Docker Compose.**

## Основные компоненты системы

1. **Cервер WSGI/ASGI** — сервер с запущенным приложением.
2. **Nginx** — прокси-сервер, который является точкой входа для web-приложения.
3. **PostgreSQL** — реляционное хранилище данных. 

## Схема сервиса

![all](images/all.png)

## Запуск приложения

Запуск всех компонентов приложения осуществляется через **Docker Compose.** 
Перед запуском необходимо определить нужные переменные среды в файлах `.env.prod` (для приложения админки) и `.env.db.prod` (для базы данных).
В качестве примера в репозитории лежат соответственно файлы `.env.prod.sample` и `.env.db.prod.sample`.  
Таким образом, запуск приложения выглядит так:

    $ cp .env.prod.sample .env.prod 
    $ cp .env.db.prod.sample .env.db.prod 
    {опционально: настроить переменные окружения в файлах} 
    $ docker-compose up -d --build 

