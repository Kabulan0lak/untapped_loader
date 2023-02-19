def read_and_load_emprego_raw_data(full_refresh: bool = False, n_days: int = 3):
    """TODO"""
    # Just print that we run this job with this mode
    mode = "full_refresh" if full_refresh else f"incremental ({n_days} days)"
    print(f"Running emprego_job with mode {mode}")
