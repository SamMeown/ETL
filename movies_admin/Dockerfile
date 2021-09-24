FROM python:3.9.4-alpine

ENV APP_HOME=/usr/src/app
WORKDIR $APP_HOME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk update \
    && apk add postgresql-dev gcc python3-dev musl-dev

COPY ./requirements/base.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY ./entrypoint.sh .
RUN sed -i 's/\r$//g' $APP_HOME/entrypoint.sh
RUN chmod +x $APP_HOME/entrypoint.sh

COPY . .

EXPOSE 8000

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["gunicorn", "config.wsgi:application", "--workers", "3", "--log-level", "info", "--access-logfile", "-", "--bind", "0.0.0.0:8000"]
