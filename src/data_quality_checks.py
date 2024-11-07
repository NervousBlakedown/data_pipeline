# src/data_quality_checks.py
import os
import logging
from dotenv import load_dotenv
import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from pipeline_error_handler import handle_error

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Great Expectations data context
context = ge.data_context.DataContext()

# Set up data validation suite
def run_data_validation():
    try:
        # Define a checkpoint for data validation
        checkpoint = SimpleCheckpoint(
            name="data_quality_checkpoint",
            data_context=context,
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "my_s3_datasource",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": "s3://my-bucket/transformed/sessionized/part-00000.parquet",
                    },
                    "expectation_suite_name": "pipeline_data_suite",
                }
            ],
        )

        # Run validation
        result = checkpoint.run()

        # Check validation result
        if result["success"]:
            logger.info("Data validation passed.")
        else:
            logger.warning("Data validation failed. Logging errors.")
            handle_error(ValueError("Data quality validation failed."), "DATA_VALIDATION")

    except Exception as e:
        handle_error(e, "DATA_VALIDATION")

if __name__ == "__main__":
    run_data_validation()
