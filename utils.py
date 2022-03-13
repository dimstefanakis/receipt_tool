import pandas as pd


def csv_to_json(path):
    df = pd.read_json(path)
    return df.to_csv


def json_to_csv(file):
    pass
