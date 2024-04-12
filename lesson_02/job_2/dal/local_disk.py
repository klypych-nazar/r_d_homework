import os
import shutil
import json


def read_from_disk(raw_dir: str) -> dict:
    """
    Reads a JSON file from a specified directory and returns its contents as a dictionary.

    Args:
        raw_dir (str): The directory path where the JSON file is stored. The function
                       assumes the file name to be the same as the last part of the path,
                       with '.json' as the extension.

    Returns:
        dict: The contents of the JSON file converted into a dictionary.

    Raises:
        FileNotFoundError: If the JSON file does not exist at the specified path.
        JSONDecodeError: If the file is not a valid JSON or if the file cannot be decoded.
    """
    file_path = add_file_name_to_path(raw_dir, 'json')
    try:
        with open(file_path, 'rb') as file:
            return json.load(file)
    except FileNotFoundError as e:
        print('No raw data was found for this date. Staging will be skipped...')
        return {}

def manage_directories(directory: str) -> None:
    """
    Ensures a specified directory is empty by deleting all its contents if it already exists,
    or creates the directory if it does not exist.

    Args:
        directory (str): The path to the directory that needs to be managed. If the directory
                         does not exist, it will be created. If it exists, all contents will
                         be removed.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')


def add_file_name_to_path(path: str, file_type: str = 'json') -> str:
    """
    Appends a file name derived from the last segment of a path with a specified file type to the path.

    Args:
        path (str): The base path where the file is to be created. The function derives the file
                    name from the last segment of this path.
        file_type (str): The extension to append to the derived file name. Default is 'json'.

    Returns:
        str: The complete file path constructed by appending the derived file name with the
             specified extension to the provided path.

    Examples:
        add_file_name_to_path("/path/to/directory", "json") returns "/path/to/directory/directory.json"
    """
    base = os.path.basename(path)
    file_name = f'{base}.{file_type}'
    full_path = os.path.join(path, file_name)
    return full_path
