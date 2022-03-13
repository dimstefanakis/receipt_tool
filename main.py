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


def analyze_receipt(images, filename):
    headers = {
        'CLIENT-ID': 'vrfdfGFuYPvUX36YzRkDNeRMJINLcm8fDhsnnxs',
        'AUTHORIZATION': 'apikey jim.productlab:89a124f66f5d471fd0d9f6f24cc7fc70'
    }
    data = {
        'file_urls': images,
        'file_name': filename,
        'external_id': filename
    }
    url = 'https://api.veryfi.com/api/v7/partner/documents/'

    response = requests.post(url, headers=headers, json=data)

    data = response.json()
    with open(f"parsed_receipt_data/{filename}.json", 'w') as outfile:
        json.dump(data, outfile)


def get_csv_from_export():
    with open('export.csv') as export:
        csv_reader = csv.DictReader(export)
        for row in list(csv_reader)[0:5]:
            datetime_object = datetime.fromisoformat(row['submission_time'])
            ts = (datetime_object - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

            images = row['images'].split('|')
            image_filename = f"{row['user_id'].replace('-', '')}-{row['id'].replace('-', '')}"
            # analyze_receipt(images, image_filename)

            with open('parsed_receipts.json') as parsed_receipts:
                parsed_receipts_list = json.load(parsed_receipts)

            # update list of receipts that have already been parsed
            parsed_receipt = {
                'id': row['id'],
                'submission_time': str(ts)
            }

            receipt_has_already_been_parsed = False
            for receipt in parsed_receipts_list:
                if receipt['id'] == parsed_receipt['id']:
                    receipt_has_already_been_parsed = True

            if not receipt_has_already_been_parsed:
                parsed_receipts_list.append(parsed_receipt)

            with open('parsed_receipts.json', 'w') as parsed_receipts:
                json.dump(parsed_receipts_list, parsed_receipts)
            print(row)


if __name__ == '__main__':
    get_csv_from_export()
