from django.urls import path

from api.v1 import views


urlpatterns = [
    path('movies/', views.MoviesListApi.as_view()),
]
