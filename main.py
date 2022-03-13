import requests
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from utils import csv_to_json, json_to_csv
from datetime import datetime
import pytz
import json
import csv


# engine = create_engine('bigquery://')
# table = Table('dataset.table', MetaData(bind=engine), autoload=True)
# print(select([func.count('*')], from_obj=table).scalar())


def analyze_receipt(image, filename):
    headers = {
        'CLIENT-ID': 'vrfdfGFuYPvUX36YzRkDNeRMJINLcm8fDhsnnxs',
        'AUTHORIZATION': 'apikey jim.productlab:89a124f66f5d471fd0d9f6f24cc7fc70'
    }
    data = {
        'file_url': image,
        'file_name': filename
    }
    url = 'https://api.veryfi.com/api/v7/partner/documents/'

    response = requests.post(url, headers=headers, json=data)

    data = response.json()
    with open(f"parsed_receipt_data/{filename}.json", 'w') as outfile:
        json.dump(data, outfile)


def get_csv_from_export():
    with open('export.csv') as export, open('parsed_receipts.json') as parsed_receipts:
        csv_reader = csv.DictReader(export)
        for row in list(csv_reader)[0:5]:
            images = row['images'].split('|')
            for count, image in enumerate(images):
                datetime_object = datetime.fromisoformat(row['submission_time'])

                # this is the naive timestamp
                # created_timestamp = int((datetime_object.replace(tzinfo=None) - datetime(1970, 1, 1)).total_seconds())

                ts = (datetime_object - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
                image_filename = f"{row['user_id'].replace('-', '')}-{row['id'].replace('-', '')}-{count}-{str(ts).replace('.', '|')}"
                print(image_filename)
                parsed_receipt = {
                    'id': row['id'],
                    'submission_time': str(ts)
                }
                # analyze_receipt(image, image_filename)
            print(row)


if __name__ == '__main__':
    get_csv_from_export()
