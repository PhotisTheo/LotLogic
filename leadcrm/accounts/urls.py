from django.contrib.auth import views as auth_views
from django.urls import path

from . import views
from .forms import StyledAuthenticationForm

app_name = "accounts"

urlpatterns = [
    path("signup/", views.signup, name="signup"),
    path("terms/", views.TermsView.as_view(), name="terms"),
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
    path("settings/", views.SettingsView.as_view(), name="settings"),
    path("settings/password/", views.change_password, name="change_password"),
    path("settings/delete/", views.delete_account, name="delete_account"),
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
    path(
        "beta/approve/<uuid:token>/",
        views.beta_request_approve,
        name="beta_request_approve",
    ),
    path(
        "confirm-email/<uuid:token>/",
        views.confirm_email,
        name="confirm_email",
    ),
    path("signup/payment-intent/", views.signup_payment_intent, name="signup_payment_intent"),
]
