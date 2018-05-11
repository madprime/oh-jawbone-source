from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('complete/', views.complete, name='complete'),
    path('jawbone_complete/', views.jawbone_complete, name='jawbone_complete'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('update_data/', views.update_data, name='update_data'),
    path('remove_jawbone/', views.remove_jawbone, name='remove_jawbone')
]
