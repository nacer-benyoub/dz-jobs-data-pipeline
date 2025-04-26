from datetime import datetime, timedelta
import requests
from dz_jobs_aggregator.utils import parse_emploitic_json
import time
import pandas as pd

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load data from Emploitic API
    """

    NUM_LISTINGS = kwargs.get("NUM_LISTINGS", 100)
    PAGE = kwargs.get("PAGE", 1)
    backfill = kwargs.get("backfill", False)
    print(f"backfill: {backfill}")
    ds = kwargs["interval_start_datetime"]
    print(f"ds: {ds}")
    # timestamp is needed because it's the only datetime format the API uses for date filtering

    if backfill:
        ds_ts = int(ds.timestamp() * 1000)
        next_ds = kwargs.get("interval_end_datetime")
        print(f"next_ds: {next_ds}")
        next_ds_ts = int(next_ds.timestamp() * 1000)
        params_filter = f"(publishedAt_timestamp>={ds_ts}) AND (publishedAt_timestamp<{next_ds_ts})",  # get all jobs publsihed within the backfill interval
    else:
        # truncate to the start of day to get fresh job listings
        ds_ts = int(datetime(ds.year, ds.month, ds.day).timestamp() * 1000)
        yesterday_ds = ds - timedelta(days=1)
        print(f"yesterday_ds: {yesterday_ds}")
        yesterday_ds_ts = int(
            datetime(yesterday_ds.year, yesterday_ds.month, yesterday_ds.day).timestamp() * 1000
        )
        params_filter = f"(publishedAt_timestamp<{ds_ts}) AND (publishedAt_timestamp>={yesterday_ds_ts})",  # get only jobs publsihed in the last day
    
    BASE_URL = "https://emploitic.com/api/v4/jobs"
    params = {
        "sort[0]": "publishedAt_timestamp:desc",
        "filter": params_filter,
        "pagination[page]": PAGE,
        "pagination[pageSize]": NUM_LISTINGS,
    }
    response = requests.get(BASE_URL, params=params)
    response_json = response.json()
    print(f"jobs to fetch: {response_json['pagination']['total']}")
    print(response.url)
    total_pages = response_json["pagination"]["totalPages"]
    jobs = parse_emploitic_json(response_json)
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            params["pagination[page]"] = page
            response = requests.get(BASE_URL, params=params)
            print(response.url)
            response_json = response.json()
            current_page_jobs = parse_emploitic_json(response_json)
            jobs.extend(current_page_jobs)
            time.sleep(5)
    df = pd.DataFrame(jobs)
    return df


@test
def test_output(output, *args, **kwargs) -> None:
    """
    Test the returned data.
    """
    assert output is not None, "The output is undefined"
    # assert output.shape[0], f"The output has no rows"
