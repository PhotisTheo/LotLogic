from django.urls import path

from . import views

urlpatterns = [
    path("", views.parcel_search_home, name="parcel_search"),
    path(
        "search/save-list/",
        views.parcel_search_save_list,
        name="parcel_search_save_list",
    ),
    path(
        "search/parcel/<int:town_id>/<slug:loc_id>/",
        views.parcel_search_detail,
        name="parcel_detail",
    ),
    path(
        "search/parcel/<int:town_id>/<slug:loc_id>/from-list/<int:list_id>/",
        views.parcel_search_detail,
        name="parcel_detail_from_list",
    ),
    path(
        "mailer/download/<int:town_id>/<str:loc_id>/",
        views.mailer_download_pdf,
        name="mailer_download_pdf",
    ),
    path(
        "search/parcel/<int:town_id>/<slug:loc_id>/save/",
        views.parcel_save_as_lead,
        name="parcel_save_as_lead",
    ),
    path(
        "api/parcel/<int:town_id>/lists/",
        views.parcel_get_user_lists,
        name="parcel_get_user_lists",
    ),
    path(
        "api/parcel/<int:town_id>/<slug:loc_id>/add-to-list/",
        views.parcel_add_to_list,
        name="parcel_add_to_list",
    ),
    path(
        "schedule-call/<int:town_id>/<slug:loc_id>/",
        views.schedule_call_request,
        name="schedule_call_request",
    ),
    path(
        "api/town-preload/<int:town_id>/",
        views.preload_town_dataset,
        name="town_preload",
    ),
    path(
        "api/payments/skiptrace/single/",
        views.skiptrace_payment_single,
        name="skiptrace_payment_single",
    ),
    path(
        "api/payments/skiptrace/bulk/<int:pk>/",
        views.skiptrace_payment_bulk,
        name="skiptrace_payment_bulk",
    ),
    path(
        "api/skiptrace/saved-list/<int:pk>/",
        views.saved_parcel_list_skiptrace,
        name="saved_parcel_list_skiptrace",
    ),
    path(
        "api/skiptrace/parcel/<int:town_id>/<slug:loc_id>/",
        views.parcel_skiptrace,
        name="parcel_skiptrace",
    ),
    path(
        "parcel/<int:town_id>/<slug:loc_id>/refresh-liens/",
        views.parcel_refresh_liens,
        name="parcel_refresh_liens",
    ),
    path(
        "api/mailer/parcel/<int:town_id>/<slug:loc_id>/",
        views.parcel_generate_mailer,
        name="parcel_mailer_generate",
    ),
    path(
        "api/parcel/<int:town_id>/<str:loc_id>/attom/",
        views.parcel_unit_attom,
        name="parcel_unit_attom",
    ),
    path(
        "api/skiptrace/lead/<int:pk>/",
        views.lead_skiptrace,
        name="lead_skiptrace",
    ),
    path(
        "api/property-types/<int:town_id>/",
        views.property_type_choices,
        name="property_type_choices",
    ),
    path("saved-lists/", views.saved_parcel_lists, name="saved_parcel_lists"),
    path(
        "saved-lists/<int:pk>/",
        views.saved_parcel_list_detail,
        name="saved_parcel_list_detail",
    ),
    path(
        "saved-lists/<int:pk>/archive/",
        views.saved_parcel_list_archive,
        name="saved_parcel_list_archive",
    ),
    path(
        "saved-lists/<int:pk>/restore/",
        views.saved_parcel_list_restore,
        name="saved_parcel_list_restore",
    ),
    path(
        "saved-lists/<int:pk>/export/",
        views.saved_parcel_list_export,
        name="saved_parcel_list_export",
    ),
    path(
        "saved-lists/<int:pk>/mailers/",
        views.saved_parcel_list_mailers,
        name="saved_parcel_list_mailers",
    ),
    path(
        "saved-lists/<int:pk>/labels/",
        views.saved_parcel_list_labels,
        name="saved_parcel_list_labels",
    ),
    path(
        "saved-lists/<int:pk>/legal-search/",
        views.bulk_legal_search,
        name="bulk_legal_search",
    ),
    path(
        "api/parcel/<int:town_id>/<str:loc_id>/geometry/",
        views.parcel_geometry,
        name="parcel_geometry",
    ),
    path(
        "api/town-boundaries/",
        views.town_boundaries,
        name="town_boundaries",
    ),
    path(
        "api/boston-neighborhoods/",
        views.boston_neighborhoods,
        name="boston_neighborhoods",
    ),
    path(
        "api/parcels-in-viewport/",
        views.parcels_in_viewport,
        name="parcels_in_viewport",
    ),
    path(
        "api/parcel-flags/",
        views.parcel_flags,
        name="parcel_flags",
    ),
    path(
        "api/town-geojson/<int:town_id>/",
        views.town_geojson,
        name="town_geojson",
    ),
    path("crm/", views.crm_overview, name="crm_overview"),
    path("crm/<slug:city_slug>/", views.crm_city_requests, name="crm_city_requests"),
    # CRM Lead Management
    path("crm/update-stage/<int:lead_id>/", views.crm_update_lead_stage, name="crm_update_lead_stage"),
    path("crm/archive/<int:lead_id>/", views.crm_archive_lead, name="crm_archive_lead"),
    path("crm/unarchive/<int:lead_id>/", views.crm_unarchive_lead, name="crm_unarchive_lead"),
    path("crm/delete/<int:lead_id>/", views.crm_delete_lead, name="crm_delete_lead"),
    path("add/", views.lead_create, name="lead_create"),
    path("upload/", views.lead_upload, name="lead_upload"),
    path("leads/<int:pk>/", views.lead_detail, name="lead_detail"),
    # Lien Record URLs
    path(
        "lien/create/<int:town_id>/<slug:loc_id>/",
        views.lien_create,
        name="lien_create",
    ),
    path("lien/<int:pk>/edit/", views.lien_edit, name="lien_edit"),
    path("lien/<int:pk>/delete/", views.lien_delete, name="lien_delete"),
    # Legal Action URLs
    path(
        "legal-action/create/<int:town_id>/<slug:loc_id>/",
        views.legal_action_create,
        name="legal_action_create",
    ),
    path("legal-action/<int:pk>/edit/", views.legal_action_edit, name="legal_action_edit"),
    path("legal-action/<int:pk>/delete/", views.legal_action_delete, name="legal_action_delete"),
    # Search public sources for liens and legal actions
    path(
        "api/search-liens-legal/<int:town_id>/<slug:loc_id>/",
        views.search_liens_legal_actions,
        name="search_liens_legal_actions",
    ),
]
