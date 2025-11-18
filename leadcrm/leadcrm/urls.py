from django.contrib import admin
from django.urls import include, path
from leads import views as leads_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("admin/trigger-market-values/", leads_views.trigger_market_values_compute, name="trigger_market_values_compute"),
    path("accounts/", include("accounts.urls")),
    path("", include("leads.urls")),
]
