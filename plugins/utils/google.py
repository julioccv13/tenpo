from google.cloud import storage

from plugins.utils.ports import Singleton


class GoogleStorageClient(metaclass=Singleton):
    def __new__(cls):
        return storage.Client()


def list_blobs(bucket: str, prefix: str, **kwargs):
    return GoogleStorageClient().list_blobs(bucket, prefix=prefix, **kwargs)


def check_existence(bucket: str, prefix: str) -> bool:
    blobs = list_blobs(bucket, prefix, max_results=1)
    return len(list(blobs)) > 0


def delete_files(bucket: str, prefix: str) -> None:
    blobs = list_blobs(bucket, prefix)
    for blob in blobs:
        blob.delete()
