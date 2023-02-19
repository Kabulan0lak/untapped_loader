"""Job entry"""
import argparse

from loader_jobs.asaak_job.loader import read_and_load_asaak_raw_data
from loader_jobs.emprego_job.loader import read_and_load_emprego_raw_data
from loader_jobs.flexclub_job.loader import read_and_load_flexclub_raw_data

parser = argparse.ArgumentParser(
    usage="untapped_loader execute [--job_name] [--n_days]"
)
parser.add_argument(
    "--job_name",
    default="all",
    help="Can be 'asaak', 'emprego' or 'flexclub'",
)
parser.add_argument(
    "--n_days",
    default="3",
    help="The number of days to load. Defaults to 3. 'all' to perform a full refresh",
)

if __name__ == "__main__":
    args, unknown_args = parser.parse_known_args()

    # Get kwargs values
    job_name = vars(args)["job_name"]
    n_days = vars(args)["n_days"]

    # Is it a full refresh ?
    full_refresh = n_days == "all"

    # Parse the n_days and raise error if not > 0
    if not (full_refresh):
        try:
            n_days = int(n_days)
            if n_days < 1:
                raise
        except:
            raise Exception(
                f"Invalid n_days parameter: '{n_days}'. Must be either 'all' or an integer > 1"
            )

    else:
        n_days = 0

    # Asaak
    if job_name == "asaak":
        read_and_load_asaak_raw_data(full_refresh, n_days)

    # Emprego
    elif job_name == "emprego":
        read_and_load_emprego_raw_data(full_refresh, n_days)

    # Flexclub
    elif job_name == "flexclub":
        read_and_load_flexclub_raw_data(full_refresh, n_days)

    # All
    elif job_name == "all":
        read_and_load_asaak_raw_data(full_refresh, n_days)
        read_and_load_emprego_raw_data(full_refresh, n_days)
        read_and_load_flexclub_raw_data(full_refresh, n_days)

    # Unknown job name
    else:
        raise Exception("Unkown job_name: '{}'".format(vars(args)["job_name"]))
