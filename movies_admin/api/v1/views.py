from django.http import JsonResponse
from django.views.generic.list import BaseListView
from django.db.models import Q, OuterRef, Subquery
from django.contrib.postgres.aggregates import ArrayAgg

from movies.models import Filmwork, FilmworkGenre, PersonType


class MoviesListApi(BaseListView):
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        genres_sub = FilmworkGenre.objects.filter(film_work=OuterRef('pk'))\
            .values('film_work')\
            .annotate(genres=ArrayAgg('genre__name'))\
            .values_list('genres')
        query_set = Filmwork.objects.values('id', 'title', 'description', 'creation_date', 'rating', 'type')\
            .annotate(actors=ArrayAgg('persons__full_name', filter=Q(filmworkperson__role=PersonType.ACTOR)),
                      directors=ArrayAgg('persons__full_name', filter=Q(filmworkperson__role=PersonType.DIRECTOR)),
                      writers=ArrayAgg('persons__full_name', filter=Q(filmworkperson__role=PersonType.WRITER)))\
            .annotate(genres=Subquery(genres_sub))
        return query_set

    def get_context_data(self, *, object_list=None, **kwargs):
        context = {
            'results': list(self.get_queryset())
        }
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)
