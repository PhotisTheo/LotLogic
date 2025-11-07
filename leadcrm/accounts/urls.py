from django.contrib.auth import views as auth_views
from django.urls import path

from . import views
from .forms import StyledAuthenticationForm

app_name = "accounts"

urlpatterns = [
    path("signup/", views.signup, name="signup"),
    path(
        "login/",
        auth_views.LoginView.as_view(
            template_name="accounts/login.html",
            authentication_form=StyledAuthenticationForm,
        ),
        name="login",
    ),
    path("logout/", auth_views.LogoutView.as_view(), name="logout"),
    path("profile/", views.ProfileView.as_view(), name="profile"),
    path(
        "mailers/templates/",
        views.MailerTemplateListView.as_view(),
        name="mailer_templates",
    ),
    path(
        "mailers/templates/<int:pk>/edit/",
        views.mailer_template_edit,
        name="mailer_template_edit",
    ),
    path(
        "mailers/templates/<int:pk>/delete/",
        views.mailer_template_delete,
        name="mailer_template_delete",
    ),
    path("team/invite/", views.team_invite_create, name="team_invite_create"),
    path(
        "team/invite/<uuid:token>/accept/",
        views.team_invite_accept,
        name="team_invite_accept",
    ),
    path("signup/payment-intent/", views.signup_payment_intent, name="signup_payment_intent"),
]
