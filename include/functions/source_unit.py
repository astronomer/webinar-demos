import os

def get_local_files_source_units(uri):
    path = uri.split("file://")[1]
    folders = os.listdir(path)
    folders = [os.path.join(uri, folder) for folder in folders]
    return folders


def get_gh_repos(list_of_repos):
    return list_of_repos
    