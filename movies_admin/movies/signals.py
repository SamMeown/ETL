from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete
from typing import Tuple
import uuid
from datetime import datetime

from .models import Filmwork


# С помощью сигналов мы детектим изменения связей кинопроизведений с персонами и жанрами, определяем фильмы, которые эти
# изменения затронут и обновляем в Filmwork updated_at у соответствующего фильма. Далее ETL периодически мониторит эту
# таблицу и при появлении новых изменений обновляет информацию о соответствующих фильмах в Elasticsearch'e.


def update_filmwork_updates(film_ids: Tuple[uuid.UUID]):
    updated = Filmwork.objects.filter(id__in=film_ids).update(updated_at=datetime.now())
    print(updated)


@receiver(post_save, sender='movies.FilmworkPerson')
def on_filmwork_person_save(sender, instance, **kwargs):
    print("on_filmwork_person_save:", instance)
    update_filmwork_updates((instance.film_work_id,))


@receiver(post_save, sender='movies.FilmworkGenre')
def on_filmwork_genre_save(sender, instance, **kwargs):
    print("on_filmwork_genre_save:", instance)
    update_filmwork_updates((instance.film_work_id,))


@receiver(pre_delete, sender='movies.FilmworkPerson')
def on_filmwork_person_delete(sender, instance, **kwargs):
    print("on_filmwork_person_delete:", instance)
    update_filmwork_updates((instance.film_work_id,))


@receiver(pre_delete, sender='movies.FilmworkGenre')
def on_filmwork_genre_delete(sender, instance, **kwargs):
    print("on_filmwork_genre_delete:", instance)
    update_filmwork_updates((instance.film_work_id,))
