import json
import logging
import os
from pprint import pprint
from mage_ai.data_preparation.logging.logger import DictLogger
from mage_ai.data_preparation.logging.logger_manager_factory import LoggerManagerFactory
import requests

from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.triggers.utils import check_pipeline_run_status
    
TRIGGER_URL = "http://localhost:6789/api/pipeline_schedules/1/pipeline_runs/4f2159a3b8fc41148d602aa052b88e5f"

def trigger_pipeline() -> dict:
    """Trigger a pipeline run and return the run_id."""
    logger.info("Calling the API trigger...")
    resp = requests.post(TRIGGER_URL)
    response_json = resp.json()
    
    # raise execption if error encountered
    err = response_json.get('error')
    if err:
        logger.error(err.get("message"))
        raise Exception(err.get("exception"))
    
    run_data = response_json.get("pipeline_run")
    if not run_data:
        logger.error(f"Missing pipeline_run key in trigger response. Got {response_json}")
        raise ValueError(f"Missing pipeline_run key in trigger response. Got {response_json}")
    run_id = run_data.get("id")
    logger.info(f"Pipeline run {run_id} created")
    return run_id



def wait_for_completion(run_id: int, poll_interval: int = 10) -> PipelineRun:
    """Poll the run status until it completes, times out or fails."""
    if not run_id:
        raise ValueError("Missing run ID.")

    # get the run onject
    db_connection.start_session()
    pipeline_run = PipelineRun.query.get(run_id)
    
    # poll the run status
    logger.info("Polling the run status...")
    pipeline_run = check_pipeline_run_status(
        pipeline_run=pipeline_run,
        poll_interval=poll_interval,
        poll_timeout=300,
        verbose=True
    )
    return pipeline_run


if __name__ == "__main__":
    # Setup logging
    fmt = "%(asctime)s %(filename)s - %(levelname)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=logging.INFO, format=fmt, datefmt=datefmt, force=True)
    logger = logging.getLogger(__name__)
    
    # cd into the project directory to avoid mage context issues
    os.chdir(os.environ.get("USER_CODE_PATH"))

    # make a post request to the API trigger to start the run
    run_id = trigger_pipeline()

    # poll the run details until it's completed
    pipeline_run = wait_for_completion(run_id, poll_interval=15)
    
    # raise error if pipeline run failed
    status = pipeline_run.status.value
    if status in [
            PipelineRun.PipelineRunStatus.FAILED.value,
            PipelineRun.PipelineRunStatus.CANCELLED.value,
    ]:
        message = f'Pipeline run {pipeline_run.id} for pipeline {pipeline_run.pipeline_uuid}: {status}.'
        logger.error(message)
        raise Exception(message)