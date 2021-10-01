from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete
from typing import Tuple
import uuid
from datetime import datetime

from .models import ETLUpdates, FilmworkPerson, FilmworkGenre


# Для отслеживания изменения кинопроизведений, персон, жанров и их связей мы завели в бд отдельную табличку (см.
# ETLUpdates). С помощью сигналов мы детектим все такие изменения, определяем фильмы, которые эти изменения затронут
# и обновляем/добавляем в ETLUpdates updated_at у соответствующего фильма. Далее ETL периодически мониторит эту таблицу
# и при появлении новых изменений обновляет информацию о соответствующих фильмах в Elasticsearch'e.


def update_etl_updates(film_ids: Tuple[uuid.UUID]):
    updated = ETLUpdates.objects.filter(film_work_id__in=film_ids).update(updated_at=datetime.now())
    updates = []
    if not updated:
        for film_id in film_ids:
            updates.append(ETLUpdates(film_work_id=film_id, updated_at=datetime.now()))
        ETLUpdates.objects.bulk_create(updates)
    print(updates, updated)


def film_ids_with_instance(model, instance, relation_model):
    field = model._meta.object_name.lower()
    ids = relation_model.objects.filter(**{field: instance}).values_list('film_work_id', flat=True)
    ids = tuple(set(ids))
    return ids


@receiver(post_save, sender='movies.Filmwork')
def on_filmwork_save(sender, instance, **kwargs):
    print("on_filmwork_save:", instance)
    update_etl_updates((instance.id,))


@receiver(post_save, sender='movies.Person')
def on_person_save(sender, instance, **kwargs):
    print("on_person_save:", instance)
    ids = film_ids_with_instance(sender, instance, FilmworkPerson)
    update_etl_updates(ids)


@receiver(post_save, sender='movies.Genre')
def on_genre_save(sender, instance, **kwargs):
    print("on_genre_save:", instance)
    ids = film_ids_with_instance(sender, instance, FilmworkGenre)
    update_etl_updates(ids)


@receiver(post_save, sender='movies.FilmworkPerson')
def on_filmwork_person_save(sender, instance, **kwargs):
    print("on_filmwork_person_save:", instance)
    update_etl_updates((instance.film_work_id,))


@receiver(post_save, sender='movies.FilmworkGenre')
def on_filmwork_genre_save(sender, instance, **kwargs):
    print("on_filmwork_genre_save:", instance)
    update_etl_updates((instance.film_work_id,))


@receiver(pre_delete, sender='movies.Filmwork')
def on_filmwork_delete(sender, instance, **kwargs):
    print("on_filmwork_delete:", instance)
    update_etl_updates((instance.id,))


@receiver(pre_delete, sender='movies.Person')
def on_person_delete(sender, instance, **kwargs):
    print("on_person_delete:", instance)
    ids = film_ids_with_instance(sender, instance, FilmworkPerson)
    update_etl_updates(ids)


@receiver(pre_delete, sender='movies.Genre')
def on_genre_delete(sender, instance, **kwargs):
    print("on_genre_delete:", instance)
    ids = film_ids_with_instance(sender, instance, FilmworkGenre)
    update_etl_updates(ids)


@receiver(pre_delete, sender='movies.FilmworkPerson')
def on_filmwork_person_delete(sender, instance, **kwargs):
    print("on_filmwork_person_delete:", instance)
    update_etl_updates((instance.film_work_id,))


@receiver(pre_delete, sender='movies.FilmworkGenre')
def on_filmwork_genre_delete(sender, instance, **kwargs):
    print("on_filmwork_genre_delete:", instance)
    update_etl_updates((instance.film_work_id,))
