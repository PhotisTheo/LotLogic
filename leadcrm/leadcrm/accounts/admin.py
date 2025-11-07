from django.contrib import admin

from .models import TeamInvite, UserProfile


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ("user", "account_type", "team_lead")
    search_fields = ("user__username", "user__email")
    list_filter = ("account_type",)


@admin.register(TeamInvite)
class TeamInviteAdmin(admin.ModelAdmin):
    list_display = ("email", "team_lead", "created_at", "accepted_at")
    search_fields = ("email", "team_lead__username")
    list_filter = ("accepted_at",)
