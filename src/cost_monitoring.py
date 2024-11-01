# src/cost_monitoring.py
import boto3
import os
from dotenv import load_dotenv
import datetime
import logging

load_dotenv()

cost_explorer = boto3.client(
    'ce',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"  
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_monthly_cost():
    # Calculate the start and end dates for the previous month
    today = datetime.date.today()
    start_date = today.replace(day=1) - datetime.timedelta(days=1)
    end_date = start_date.replace(day=1)
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    try:
        response = cost_explorer.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost']
        )

        # Retrieve the total cost for the specified period
        cost_data = response['ResultsByTime'][0]['Total']['UnblendedCost']['Amount']
        logger.info(f"Total cost for {start_date} to {end_date}: ${cost_data}")
        return f"Total AWS cost for the period {start_date} to {end_date}: ${cost_data}"

    except Exception as e:
        logger.error(f"Failed to retrieve cost data: {e}")
        return "Cost data retrieval failed."

if __name__ == "__main__":
    print(get_monthly_cost())
