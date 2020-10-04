from datetime import datetime
from io import StringIO
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

BUCKET_NAME = "scrapedhousingdata"
WEBSITE = "rightmove"

logger = logging.Logger(__name__)

with open(r"static/rightmove/location_params.yaml") as location_file:
    LOCATION_PARAMS = yaml.load(location_file, Loader=yaml.FullLoader)
with open(r"static/rightmove/base_params.yaml") as base_params_file:
    BASE_PARAMS = yaml.load(base_params_file, Loader=yaml.FullLoader)


def initiate_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36"  # pylint:disable=line-too-long
        }
    )
    return s


def make_rightmove_request(location, location_value, params):
    url = "https://www.rightmove.co.uk/api/_search"
    session = initiate_session()

    params["locationIdentifier"] = location_value

    logger.info(f"making request: {location}")
    r = session.get(url, params=params)

    if r.status_code == 200:
        return r.json()
    return "INVALID REQUEST"


def create_raw_file_location(state, website, location, file_type, index=24):
    file_path = os.path.join(
        state,
        "property_data",
        website,
        datetime.today().date().strftime("%Y/%m/%d"),
        str(index),
        location + "_" + str(index) + "." + file_type,
    )
    return file_path


def create_trans_file_location(state, data_name, file_type):
    file_path = os.path.join(
        state,
        "property_data",
        WEBSITE,
        datetime.today().date().strftime("%Y/%m/%d"),
        data_name,
        data_name + "." + file_type,
    )
    return file_path


def get_properties(wait_time, save_files=False):
    df = pd.DataFrame()
    for k, v in LOCATION_PARAMS.items():
        no_properties, index_range = None, []
        if not no_properties:
            json_response = make_rightmove_request(k, v, BASE_PARAMS)
            if save_files:
                s3_client.put_object(
                    Body=json.dumps(json_response),
                    Bucket=BUCKET_NAME,
                    Key=create_raw_file_location(
                        state="raw", website=WEBSITE, location=k, file_type="json"
                    ),
                )
            location_property_data = pd.DataFrame(json_response["properties"])
            no_properties = json_response["resultCount"]
            index_range += [
                i for i in range(48, int(no_properties), 24) if int(no_properties)
            ]
        if index_range:
            for i in index_range:
                time.sleep(wait_time)
                BASE_PARAMS["index"] = i
                json_response = make_rightmove_request(k, v, BASE_PARAMS)
                if save_files:
                    s3_client.put_object(
                        Body=json.dumps(json_response),
                        Bucket=BUCKET_NAME,
                        Key=create_raw_file_location(
                            state="raw",
                            website=WEBSITE,
                            location=k,
                            file_type="json",
                            index=i,
                        ),
                    )
                location_property_data = location_property_data.append(
                    pd.DataFrame(json_response["properties"]), ignore_index=True
                )
                BASE_PARAMS.pop("index")
        location_property_data["potential_location"] = k
        df = df.append(location_property_data, ignore_index=True)
        BASE_PARAMS.pop("locationIdentifier")
    return df


def format_property_data(df):
    # expading dicts in columns
    df = pd.concat([df, property_data["location"].apply(pd.Series)], axis=1)
    df = pd.concat([df, property_data["listingUpdate"].apply(pd.Series)], axis=1)
    df = pd.concat(
        [df, property_data["price"].apply(pd.Series)[["currencyCode", "amount"]]],
        axis=1,
    )

    # expanding columns into useful datapoints
    df = pd.concat(
        [
            df,
            df["addedOrReduced"]
            .str.split(" on ", expand=True)
            .rename(columns={0: "addedReduced", 1: "addedReducedDate"}),
        ],
        axis=1,
    )

    # saving potentially interesting information to other dataframes with listing id as PK
    estate_agents = pd.concat([df["id"], df["customer"].apply(pd.Series)], axis=1)
    images = pd.concat(
        [
            df["id"],
            df["propertyImages"]
            .apply(pd.Series)["images"]
            .apply(lambda x: [y["srcUrl"] for y in x]),
        ],
        axis=1,
    )

    # remove either irrelevant columns or those that have been converted from dicst above
    df = df.drop(
        columns=[
            "location",
            "listingUpdate",
            "price",
            "customer",
            "productLabel",
            "propertyImages",
            "addedOrReduced",
        ]
    )

    # convert date strings to datetime objects
    df["firstVisibleDate"] = pd.to_datetime(
        df["firstVisibleDate"], format="%Y-%m-%dT%H:%M:%SZ"
    )
    df["listingUpdateDate"] = pd.to_datetime(
        df["listingUpdateDate"], format="%Y-%m-%dT%H:%M:%SZ"
    )
    df["addedReducedDate"] = pd.to_datetime(df["addedReducedDate"], format="%d/%m/%Y")

    return df, estate_agents, images


def upload_trans_files(dfs):
    for df, data_name in zip(
        dfs,
        ["property_details", "estate_agent_details", "property_images"],
    ):
        file_path = create_trans_file_location(
            state="trans", data_name=data_name, file_type="csv"
        )
        upload_to_s3(df, file_path)


def upload_to_s3(data, key):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME, Key=key)


if __name__ == "__main__":
    property_data = get_properties(2, save_files=True)
    formatted_property_df, estate_agent_df, image_df = format_property_data(
        property_data
    )
    upload_trans_files([formatted_property_df, estate_agent_df, image_df])
