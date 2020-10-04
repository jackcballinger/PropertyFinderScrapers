from datetime import datetime
import json
import logging
import os
import time

import boto3
import pandas as pd
import requests
import yaml

s3_session = boto3.Session()
s3_client = s3_session.client("s3")

API_KEY = os.environ["zoopla_api_key"]
BUCEKT_NAME = "scrapedhousingdata"
WEBSITE = "zoopla"

logger = logging.Logger(__name__)

with open(r"static/zoopla/location_params.yaml") as location_file:
    LOCATION_PARAMS = yaml.load(location_file, Loader=yaml.FullLoader)
with open(r"static/zoopla/base_params.yaml") as base_params_file:
    BASE_PARAMS = yaml.load(base_params_file, Loader=yaml.FullLoader)

BASE_PARAMS["api_key"] = API_KEY


def initiate_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36"  # pylint:disable=line-too-long
        }
    )
    return s


def make_zoopla_request(location, location_value, params):
    url = "https://api.zoopla.co.uk/api/v1/property_listings.js"
    session = initiate_session()

    params["area"] = location_value

    logger.info(f"making request: {location}")
    r = session.get(url, params=BASE_PARAMS)

    if r.status_code == 200:
        return r.json()
    return "INVALID REQUEST"


def create_raw_file_location(state, location, file_type, page_no=1):
    file_path = os.path.join(
        state,
        "property_data",
        WEBSITE,
        datetime.today().date().strftime("%Y/%m/%d"),
        str(page_no),
        location + "_" + str(page_no) + "." + file_type,
    )
    return file_path.replace("\\", "/")


def upload_to_s3(data, key):
    s3_client.put_object(Body=data, Bucket=BUCEKT_NAME, Key=key)


def get_properties(wait_time, save_files=True):
    df = pd.DataFrame()
    for k, v in LOCATION_PARAMS.items():
        print(k)
        no_properties = None
        json_response = make_zoopla_request(k, v, BASE_PARAMS)
        if save_files:
            upload_to_s3(
                json.dumps(json_response),
                create_raw_file_location(state="raw", location=k, file_type="json"),
            )
        df = df.append(pd.DataFrame(json_response["listing"]), ignore_index=True)
        no_properties = json_response["result_count"]
        if no_properties > 100:
            for page_no in range(2, int(no_properties / 100) + 2):
                time.sleep(wait_time)
                BASE_PARAMS["page_number"] = page_no
                json_response = make_zoopla_request(k, v, BASE_PARAMS)
                if save_files:
                    upload_to_s3(
                        json.dumps(json_response),
                        create_raw_file_location(
                            state="raw", location=k, file_type="json", page_no=page_no
                        ),
                    )
                df = df.append(
                    pd.DataFrame(json_response["listing"]), ignore_index=True
                )
                BASE_PARAMS.pop("page_number")
        BASE_PARAMS.pop("area")
    return df


property_data = get_properties(2)
