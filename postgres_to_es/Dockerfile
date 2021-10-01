FROM python:3.9.4-alpine

ENV APP_HOME=/usr/src/postgres_to_es
WORKDIR $APP_HOME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk update \
    && apk add postgresql-dev gcc python3-dev musl-dev

COPY ./requirements/base.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

ENV PYTHONPATH $APP_HOME/..

COPY . .

CMD python3 etl.py
