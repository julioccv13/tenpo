from typing import Optional

from google.cloud import storage


client = storage.Client()


def get_bucket_and_blob_from_path(path: str):
    path_list = path.split("/")
    bucket_name = path_list[2]
    blob_name = "/".join(path_list[3:])
    return bucket_name, blob_name


def get_blob_from_path(path: str):
    bucket_name, blob_name = get_bucket_and_blob_from_path(path)
    bucket = client.get_bucket(bucket_name)
    return bucket.blob(blob_name)


def get_blobs_list_prefix_path(path: str, **kwargs):
    bucket_name, blob_name = get_bucket_and_blob_from_path(path)
    bucket = client.get_bucket(bucket_name)
    return bucket.list_blobs(prefix=blob_name, **kwargs)


def get_blobs_list_prefix_path_as_strings(path: str, **kwargs):
    list_of_blobs = get_blobs_list_prefix_path(path, **kwargs)
    return [(p.name, p.download_as_string().decode("utf-8")) for p in list_of_blobs]


def prefix_exists(path: str) -> bool:
    blobs_list = get_blobs_list_prefix_path(path, max_results=1)
    return len(list(blobs_list)) > 0


def check_storage_path_exists(path: str) -> bool:
    blob = get_blob_from_path(path)
    return blob.exists()


def delete_files(path: str, recursive: Optional[bool] = False) -> None:
    if recursive:
        blobs_list = get_blobs_list_prefix_path(path)
    else:
        blobs_list = [get_blob_from_path(path)]

    for blob in blobs_list:
        blob.delete()


def get_blob_as_string(path: str) -> str:
    blob = get_blob_from_path(path)
    return blob.download_as_string().decode("utf-8")


def move_files(path_from: str, path_to: str):
    source_bucket_name, source_blob = get_bucket_and_blob_from_path(path_from)
    source_bucket = client.get_bucket(source_bucket_name)
    destination_bucket_name, destination__blob = get_bucket_and_blob_from_path(path_to)
    destination_bucket = client.bucket(destination_bucket_name)
    blobs_list = get_blobs_list_prefix_path(path_from)
    for blob in blobs_list:
        if blob.name != source_blob:
            destination_blob_name = destination__blob + str(blob.name).replace(
                source_blob, ""
            )
            source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
            blob.delete()


def copy_files(path_from: str, path_to: str):
    source_bucket_name, source_blob = get_bucket_and_blob_from_path(path_from)
    source_bucket = client.get_bucket(source_bucket_name)
    destination_bucket_name, destination__blob = get_bucket_and_blob_from_path(path_to)
    destination_bucket = client.bucket(destination_bucket_name)
    blobs_list = get_blobs_list_prefix_path(path_from)
    for blob in blobs_list:
        if blob.name != source_blob:
            destination_blob_name = destination__blob + str(blob.name).replace(
                source_blob, ""
            )
            source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
