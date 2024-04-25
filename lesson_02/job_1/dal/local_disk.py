import os
import shutil
import json


def save_to_disk(json_content: list[dict[str, str | int]], path: str) -> None:
    """
    Saves JSON content to a file, cleaning the directory before saving.

    Args:
        json_content (list[dict[str, str | int]]): The JSON content to save.
        path (str): The file path where the JSON content should be saved. Includes both the directories and the file name.
    """
    # Extract directory from path
    directory = os.path.dirname(path)

    # Check if the directory exists and clean it OR create it if it doesn't
    manage_directories(directory)

    # Save the JSON content to the specified path
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(json_content, f, ensure_ascii=False, indent=4)


def manage_directories(directory: str) -> None:
    """
    Ensures the given directory is empty by deleting all its contents if it already exists,
    or creates the directory if it does not exist.

    Args:
        directory (str): The path to the directory that needs to be managed.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except (OSError, FileNotFoundError, PermissionError) as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def compose_file_name(path: str, file_type: str = 'json') -> str:
    """
    Composes a complete file path by appending a file type to the base name extracted from the provided path.

    Args:
        path (str): The directory path or existing file path where the new file will be saved.
        file_type (str): The extension/type of the file to be created. Defaults to 'json'.

    Returns:
        str: The complete path for the new file, consisting of the original path and the new file name.
    """
    base = os.path.basename(path)
    file_name = f'{base}.{file_type}'
    full_path = os.path.join(path, file_name)
    return full_path
