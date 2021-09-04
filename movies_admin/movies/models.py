from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

import uuid
import functools


# Добавлям классы для текстового и файлового полей, которые пустые значения сохраняют в базу данных как NULL.
# Делаем так потому, что в базе, которую мы унаследовали, используется такое соглашение (не пустые строки, а NULL).
def return_none_if_empty(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        value = func(*args, **kwargs)
        if not value:
            return None
        return value
    return wrapper


class TextNullField(models.TextField):
    """TextField that stores NULL when empty"""
    get_db_prep_value = return_none_if_empty(models.TextField.get_db_prep_value)


class FileNullField(models.FileField):
    """FileField that stores NULL when empty"""
    get_db_prep_value = return_none_if_empty(models.FileField.get_db_prep_value)


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Genre(TimeStampedMixin, models.Model):
    """Модель для жанров кинопроизведений"""
    id = models.UUIDField(_('ID'), primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_('title'), max_length=80)
    description = models.CharField(_('description'), max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = _('genre')
        verbose_name_plural = _('genres')
        db_table = '"content"."genre"'

    def __str__(self):
        return self.name


class Person(TimeStampedMixin, models.Model):
    """Модель для персон (актеры, режиссеры, сценаристы, ...)"""
    id = models.UUIDField(_('ID'), primary_key=True, default=uuid.uuid4, editable=False)
    full_name = models.CharField(_('full name'), max_length=255)
    birth_date = models.DateField(_('birth date'), blank=True, null=True)

    class Meta:
        verbose_name = _('person')
        verbose_name_plural = _('persons')
        db_table = '"content"."person"'

    def __str__(self):
        return self.full_name


class PersonType(models.TextChoices):
    ACTOR = 'actor', _('actor')
    DIRECTOR = 'director', _('director')
    WRITER = 'writer', _('writer')


class FilmworkPerson(models.Model):
    """Модель для связи персон и кинопроизведений"""
    id = models.UUIDField(_('ID'), primary_key=True, default=uuid.uuid4, editable=False)
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.CharField(_('role'), max_length=20, choices=PersonType.choices, default=PersonType.ACTOR)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = '"content"."person_film_work"'
        unique_together = [['film_work', 'person', 'role']]


class FilmworkGenre(models.Model):
    """Модель для связи жанров и кинопроизведений"""
    id = models.UUIDField(_('ID'), primary_key=True, default=uuid.uuid4, editable=False)
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = '"content"."genre_film_work"'
        unique_together = [['film_work', 'genre']]


class FilmworkType(models.TextChoices):
    MOVIE = 'movie', _('movie')
    TV_SHOW = 'tv_show', _('tv show')


class Filmwork(TimeStampedMixin, models.Model):
    """Модель для кинопроизведения (фильма, телешоу, ...)"""
    id = models.UUIDField(_('ID'), primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(_('title'), max_length=255)
    description = TextNullField(_('description'), blank=True, null=True, default=None)
    creation_date = models.DateField(_('creation date'), blank=True, null=True)
    certificate = models.CharField(_('certificate'), max_length=255, blank=True, null=True)
    file_path = FileNullField(_('file'), upload_to='film_works/', max_length=255, blank=True, null=True, default=None)
    rating = models.FloatField(_('rating'), validators=[MinValueValidator(0), MaxValueValidator(10)],
                               blank=True, null=True)
    type = models.CharField(_('type'), max_length=20, choices=FilmworkType.choices, default=FilmworkType.MOVIE)
    genres = models.ManyToManyField(Genre, through='FilmworkGenre')
    persons = models.ManyToManyField(Person, through='FilmworkPerson')

    class Meta:
        verbose_name = _('filmwork')
        verbose_name_plural = _('filmworks')
        db_table = '"content"."film_work"'

    def __str__(self):
        return self.title
