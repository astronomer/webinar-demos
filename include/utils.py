import logging
import os

from airflow.sdk import ObjectStoragePath

logger = logging.getLogger("airflow.task")


def get_all_files(path: ObjectStoragePath) -> list[ObjectStoragePath]:
    return [f for f in path.rglob("*") if f.is_file()]

def copy_recursive(
    path_src: ObjectStoragePath,
    base_dst: ObjectStoragePath
) -> None:
    try:
        files = get_all_files(path_src)
    except FileNotFoundError:
        logger.warning(f"Source folder {path_src} not found, skipping")
        return

    for file_src in files:
        file_dest = base_dst / os.path.join(*file_src.parts[-2:])
        logger.info(f"Copying {file_src} to {file_dest}")
        try:
            file_src.copy(dst=file_dest)
        except FileNotFoundError:
            logger.warning(f"File {file_src} not found during copy, skipping")
        except Exception as e:
            logger.error(f"Error copying {file_src}: {e}")
            raise
