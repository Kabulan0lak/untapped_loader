import os
from typing import List
from datetime import datetime


def get_full_list_of_filepaths(prefix_path: str) -> List[str]:
    """Returns the file paths of all files in subdirectories of prefix_path

    Args:
        prefix_path (str): The path to the main directory to cycle through

    Returns:
        List[str]: The list of all file_paths
    """

    # Cycle through all subdirectories and store the path to each file
    file_paths = []
    for root, _, files in os.walk(prefix_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_paths.append(file_path)

    return file_paths


def get_top_n_files_of_last_month(prefix_path: str, n: int = 3) -> List[str]:
    """Returns the file paths of all files in the last month and n last days

    Args:
        prefix_path (str): The path to the directory
        n (int, optional): Number of days to retrieve. Defaults to 3.

    Returns:
        List[str]: The list of all file paths
    """

    # Find the last month and n last days
    max_m, n_days = get_max_month_and_n_last_days(prefix_path, n)

    file_paths = []

    # Cycle through all files in the max_m month and the n last days
    for day in n_days:
        file_paths += get_full_list_of_filepaths(
            os.path.join(prefix_path, str(max_m), str(day))
        )

    return file_paths


def get_max_month_and_n_last_days(file_path: str, n: int = 3):
    """Return the last month, and last n days within that month

    Args:
        file_path (str): The path to the directory
        n (int, optional): Number of days to retrieve. Defaults to 3.

    Returns:
        int, List[int]: The month and a list of days
    """
    # Find the last month
    max_m = max([int(m) for m in os.listdir(file_path)])

    # Order the days in descending orders
    days_directory = [int(d) for d in os.listdir(os.path.join(file_path, str(max_m)))]
    days_directory.sort(reverse=True)

    return max_m, days_directory[0:n]


def get_acquisition_date_from_file_path(prefix_path: str, file_path: str) -> datetime:
    """Return a datetime corresponding the 2022-mm-dd with mm and dd being the respective directories

    Args:
        prefix_path (str): The prefix path to start reading from
        file_path (str): The total file path to analyze

    Returns:
        datetime: The acquisition timestamp from the directory
    """

    month, day = file_path[file_path.index(prefix_path) + len(prefix_path) + 1 :].split(
        "/"
    )[0:2]
    acquisition_timestamp = datetime.strptime(f"2022-{month}-{day}", "%Y-%m-%d")

    return acquisition_timestamp
