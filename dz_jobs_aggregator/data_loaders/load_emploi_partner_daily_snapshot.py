from datetime import timedelta
import requests
import time
import pandas as pd

from dz_jobs_aggregator.utils import parse_emploi_partner_json

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    NUM_LISTINGS = kwargs.get("NUM_LISTINGS", 100)
    PAGE = kwargs.get("PAGE", 1)
    backfill = kwargs.get("backfill", False)
    print(f"backfill: {backfill}")
    ds = kwargs["execution_date"]
    print(f"env: {kwargs.get('env')}")
    print(f"pipeline_run_id: {kwargs.get('pipeline_run_id')}")
    print(f"execution_partition: {kwargs.get('execution_partition')}")

    if backfill:
        print(f"ds: {ds}")
        next_ds = kwargs.get("interval_end_datetime")
        print(f"next_ds: {next_ds}")
        params_filter = {
            "publishedDate[after]": ds,
            "publishedDate[strictly_before]": next_ds,
        }
    else:
        # get the daily metrics of all the jobs published within the last month to track them
        # truncate to midnight to be consistent with emlpoitic daily loader
        ds = ds.date()
        print(f"ds: {ds}")
        last_month_ds = ds - timedelta(days=30)
        print(f"last_month_ds: {last_month_ds}")
        params_filter = {
            "publishedDate[after]": last_month_ds,
            "publishedDate[strictly_before]": ds,
        }

    BASE_URL = "https://api-v4.emploipartner.com/api/jobs"
    params = {
        "order[publishedDate]": "desc",
        "_page": PAGE,
        "itemsPerPage": NUM_LISTINGS,
    } | params_filter
    response = requests.get(BASE_URL, params=params)
    print(response.url)
    response_json = response.json()
    print(f"jobs to fetch: {response_json['hydra:totalItems']}")
    jobs = parse_emploi_partner_json(response_json)

    pagination = response_json.get("hydra:view")
    if pagination and pagination.get("hydra:last"):
        total_pages = pagination.get("hydra:last").split("_page=")[-1]
        print(f"total pages: {total_pages}")
        next_endpoint = pagination.get("hydra:next")
        while next_endpoint:
            next_url_params = next_endpoint.split("/api/jobs")[-1]
            response = requests.get(BASE_URL + next_url_params)
            print(response.url)
            response_json = response.json()
            jobs += parse_emploi_partner_json(response_json)
            pagination = response_json.get("hydra:view")
            next_endpoint = pagination and pagination.get("hydra:next")
            time.sleep(5)
    else:
        print(f"total pages: 1")

    return pd.DataFrame(jobs)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"
    # assert output.shape[0], f"The output has no rows"
