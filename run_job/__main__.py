"""Job entry"""
import argparse

from loader_jobs.asaak_job.loader import read_and_load_asaak_raw_data

parser = argparse.ArgumentParser(
    usage="untapped_loader execute [--job_name] [**optional_parameters]"
)
parser.add_argument(
    "--job_name",
    default="all",
    help="Can be 'asaak', 'emprego' or 'flexclub'",
)

if __name__ == "__main__":
    args, unknown_args = parser.parse_known_args()

    if vars(args)["job_name"] == "asaak":
        read_and_load_asaak_raw_data()

    elif vars(args)["job_name"] == "all":
        print("all jobs running ...")

    # elif vars(args)["job_name"] == "da_gsheet":
    #     loader_jobs.emprego_job.loader.read_and_load_raw_data()

    # elif vars(args)["job_name"] == "oversightprivate":
    #     loader_jobs.flexclub_job.loader.read_and_load_raw_data()

    else:
        raise Exception("Unkown job_name: '{}'".format(vars(args)["job_name"]))
