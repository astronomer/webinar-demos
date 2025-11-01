import logging

from airflow.sdk import ObjectStoragePath

t_log = logging.getLogger("airflow.task")


def get_all_files(path: ObjectStoragePath) -> list[ObjectStoragePath]:
    """Recursively get all files in a directory."""
    return [f for f in path.rglob("*") if f.is_file()]


def get_all_checksums(path: ObjectStoragePath, files: list[ObjectStoragePath]) -> dict:
    """Get all checksums from a list of paths to files."""
    return {file.relative_to(path).path: file.checksum() for file in files}


def compare_checksums(
    src_checksums: dict,
    dst_checksums: dict,
    folder_name_src: str,
    folder_name_dst: str,
) -> None:
    """Compares two dicts of file checksums, raises and error in case of any mismatches"""

    checksum_mismatch = []
    for src_path, src_checksum in src_checksums.items():
        dst_path = src_path.replace(folder_name_src, folder_name_dst)
        dst_checksum = dst_checksums.get(dst_path)
        if dst_checksum is None:
            print(f"File missing in destination: {dst_path}")
            checksum_mismatch.append(f"Missing: {dst_path}")
        elif src_checksum != dst_checksum:
            print(
                f"Checksum mismatch for {dst_path}: src({src_checksum}) != dst({dst_checksum})"
            )
            checksum_mismatch.append(f"Mismatch: {dst_path}")

    if checksum_mismatch:
        raise Exception(
            "Oh no! Something went wrong, checksums did not match, stopping the pipeline!"
        )
    else:
        t_log.info("Copy from ingest to stage successful. All checksums match!")
