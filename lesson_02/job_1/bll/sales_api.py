import os

from lesson_02.job_1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    """
    Retrieves sales data for a specific date from an API and saves it to a local disk.

    Args:
        date (str): The date for which sales data needs to be retrieved. This should
                    be in the format 'YYYY-MM-DD'.
        raw_dir (str): The file system path where the sales data file should be saved.
                       This path should already exist and have write permissions.

    """
    # 1. get data from the API
    if not (data := sales_api.get_sales(date)):
        return

    path = os.path.join(raw_dir, f'{date}.json')

    # 2. save data to disk
    local_disk.save_to_disk(data, path)
