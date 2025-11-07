from django.conf import settings
from storages.backends.s3boto3 import S3Boto3Storage


class StaticStorage(S3Boto3Storage):
    """Custom storage backend for static files on S3."""
    location = settings.AWS_STATIC_LOCATION
    default_acl = None
    file_overwrite = True


class MediaStorage(S3Boto3Storage):
    """Custom storage backend for media files on S3."""
    location = settings.AWS_MEDIA_LOCATION
    default_acl = None
    file_overwrite = False
