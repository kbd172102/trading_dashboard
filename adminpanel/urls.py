# adminpanel/urls.py
from django.urls import path
from . import views

app_name = "adminpanel"

urlpatterns = [
    # Dashboard
    path("", views.admin_home, name="admin_home"),

    # Clients management
    path("clients/", views.manage_clients, name="manage_clients"),
    path("clients/add/", views.add_client, name="add_client"),
    path("clients/edit/<int:pk>/", views.edit_client, name="edit_client"),
    path("clients/delete/<int:pk>/", views.delete_client, name="delete_client"),

    # Strategy management
    path("strategies/", views.manage_strategies, name="manage_strategies"),
    path("strategies/edit/<int:strategy_id>/", views.edit_strategy, name="edit_strategy"),
    path("strategies/delete/<int:strategy_id>/", views.delete_strategy, name="delete_strategy"),

    path("clients/add-api/", views.add_client_api, name="add_client_api"),
]
