from django.contrib import admin
from .models import Filmwork, FilmworkGenre, Genre, FilmworkPerson, Person


class GenreInline(admin.TabularInline):
    model = FilmworkGenre
    extra = 0


class PersonInline(admin.TabularInline):
    model = FilmworkPerson
    extra = 0
    ordering = ('-role', 'person__full_name')


@admin.register(Filmwork)
class FilmWorkAdmin(admin.ModelAdmin):
    list_display = ('title', 'type', 'creation_date', 'rating')
    fields = (
        'title', 'type', 'description', 'creation_date', 'certificate',
        'file_path', 'rating',
    )
    inlines = (
        GenreInline,
        PersonInline,
    )

    list_filter = ('type',)
    search_fields = ('title', 'description', 'id',)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    fields = ('name', 'description')
    search_fields = ('name',)
    
    ordering = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'birth_date')
    fields = ('full_name', 'birth_date')
    search_fields = ('full_name',)

    ordering = ('full_name',)
