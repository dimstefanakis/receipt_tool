import requests
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from utils import csv_to_json, json_to_csv
from datetime import datetime
import uuid
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
    with open('exports/export.csv') as export:
        csv_reader = csv.DictReader(export)
        for row in list(csv_reader):
            datetime_object = datetime.fromisoformat(row['submission_time'])
            ts = (datetime_object - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

            images = row['images'].split('|')
            image_filename = f"{row['user_id'].replace('-', '')}-{row['id'].replace('-', '')}"

            with open('parsed_receipts.json') as parsed_receipts:
                parsed_receipts_list = json.load(parsed_receipts)

            # update list of receipts that have already been parsed
            parsed_receipt = {
                'id': row['id'],
                'external_id': image_filename,
                'submission_time': str(ts)
            }

            receipt_has_already_been_parsed = False
            for receipt in parsed_receipts_list:
                if receipt['id'] == parsed_receipt['id']:
                    receipt_has_already_been_parsed = True

            if not receipt_has_already_been_parsed:
                analyze_receipt(images, image_filename)
                parsed_receipts_list.append(parsed_receipt)

            with open('parsed_receipts.json', 'w') as parsed_receipts:
                json.dump(parsed_receipts_list, parsed_receipts)
            print(row)


def get_data_from_veryfi():
    with open('parsed_receipts.json') as parsed_receipts:
        parsed_receipts_list = json.load(parsed_receipts)
        for receipt in parsed_receipts_list:
            headers = {
                'CLIENT-ID': 'vrfdfGFuYPvUX36YzRkDNeRMJINLcm8fDhsnnxs',
                'AUTHORIZATION': 'apikey jim.productlab:89a124f66f5d471fd0d9f6f24cc7fc70'
            }

            url = f"https://api.veryfi.com/api/v7/partner/documents/?external_id={receipt['external_id']}"
            response = requests.get(url, headers=headers)

            data = response.json()
            print(data)


def get_active_veryfi_items():
    headers = {
        'CLIENT-ID': 'vrfdfGFuYPvUX36YzRkDNeRMJINLcm8fDhsnnxs',
        'AUTHORIZATION': 'apikey jim.productlab:89a124f66f5d471fd0d9f6f24cc7fc70'
    }

    url = f"https://api.veryfi.com/api/v7/partner/documents/?status=processed"
    response = requests.get(url, headers=headers)
    data = response.json()
    return data


def build_csv():
    receipts = get_active_veryfi_items()
    with open('spreadsheet.csv', 'w') as spreadsheet:
        writer = csv.writer(spreadsheet)
        writer.writerow(['user_id', 'submission_id', 'transaction_date', 'field', 'list'])
        for receipt in receipts:
            # external id is of format {user_id}-{submission_id}
            if not receipt['external_id']:
                continue

            user_id = receipt['external_id'].split('-')[0]
            submission_id = receipt['external_id'].split('-')[1]
            submission_id = uuid.UUID(hex=submission_id)

            transaction_date = receipt['date']

            # write store row
            vendor = f"{receipt['vendor']['name']}".replace('\n', ' ')
            writer.writerow([user_id, submission_id, transaction_date, 'Store', vendor])

            # write address row
            address = f"{receipt['vendor']['address']}".replace('\n', ' ')
            writer.writerow([user_id, submission_id, transaction_date, 'Address', address])

            # write total row
            total = receipt['total']
            writer.writerow([user_id, submission_id, transaction_date, 'Total', total])

            for item in receipt['line_items']:
                row_item = f"{item['description']}, {item['quantity']}, {item['total']}".replace('\n', ' ')
                writer.writerow([user_id, submission_id, transaction_date, 'Product', row_item])


if __name__ == '__main__':
    get_csv_from_export()
    get_active_veryfi_items()
    build_csv()
