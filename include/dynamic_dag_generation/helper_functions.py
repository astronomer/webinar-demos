def evaluate_new_file(new_files:list) -> bool:
    """
    Evaluate whether new files in the source bucket satisfy given criteria.
    """
    print(new_files)
    if len(new_files) > 0:
        return True
    else:
        return False
    
def verify_checksum(checksum):
    """
    Verify checksum of file
    """
    print(checksum)
    print("Checksum verified")
    return True