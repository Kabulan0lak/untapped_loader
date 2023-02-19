# List of Jobs

The current existing jobs are:

- [asaak_job](/loader_jobs/asaak_job)
- [emprego_job](/loader_jobs/emprego_job)
- [flexclub_job](/loader_jobs/flexclub_job)

# Configuration files

The configuration files must be created in the following directories :

- loader_jobs/conf.json
- loader_jobs/<job_name>/mapping.json

The `conf.json` file should define the database credentials:
```json
{
    "db_conf": {
        "host": "<host>",
        "database": "<database>",
        "user": "<user>",
        "password": "<password>",
        "port": 0000
    },
    "asaak_path": "{file_path_to_asaak_data}",
    "emprego_path": "{file_path_to_emprego_data}",
    "flexclub_path": "{file_path_to_flexclub_data}"
}
```

The `mapping.json` file should define the mapping between json attributes and database colums:
```json
{
    "table_name_1": {
        "table_column_1": "json_attribute_1",
        "table_column_2": "json_attribute_2",
    },
    "table_name_2": {
        "table_column_1": "json_attribute_1",
        "table_column_2": "json_attribute_2",
    }
}
```

# How to run?

This is a command that you have to execute to run any job :

```bash
python3 -m run_job execute --job_name=<job_name> --n_days=<n_days>
```

Example, run asaak job with the 5 last days of data:

```bash
python3 -m run_job execute --job_name=asaak --n_days=5
```
Another example, run every job in full_refresh mode:

```bash
python3 -m run_job execute --job_name=all --n_days=all
```
