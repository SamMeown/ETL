from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Filmwork, FilmworkGenre, Genre, FilmworkPerson, Person


class GenreInline(admin.TabularInline):
    model = FilmworkGenre
    extra = 0


class PersonInline(admin.TabularInline):
    model = FilmworkPerson
    extra = 0
    ordering = ('-role', 'person__full_name')


class RatingListFilter(admin.SimpleListFilter):
    title = _('rating')
    parameter_name = 'rating'

    ranges = (
        (0, 5),
        (5.1, 7),
        (7.1, 9),
        (9.1, 10)
    )

    def lookups(self, request, model_admin):
        return [('-'.join(map(str, rating_range)),
                 _(' - '.join(map(str, rating_range)))) for rating_range in self.ranges]

    def queryset(self, request, queryset):
        if not self.value():
            return
        rating_range = [float(num) for num in self.value().split('-')]
        return queryset.filter(rating__gte=rating_range[0],
                               rating__lte=rating_range[1])


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

    list_filter = ('type', RatingListFilter)
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
