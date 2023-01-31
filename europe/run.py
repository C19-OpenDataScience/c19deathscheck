#!/usr/bin/env python3
import os
import click
import requests

HERE = os.path.dirname(__file__)
DATA_DIR = os.path.join(HERE, "data")

@click.group()
def main():
    pass

@main.command("all")
def cmd_all():
    download_data()

@main.command("download_data")
def cmd_download_data():
    download_data()

def download_data():
    _mkdir(DATA_DIR)
    _download(
        'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tps00029.tsv.gz',
        os.path.join(DATA_DIR, "deaths.tsv.gz")
    )

def _mkdir(path):
    try:
        os.makedirs(path)
    except Exception:
        pass

def _download(url, ofpath):
    if not os.path.exists(ofpath):
        print(f"Download {os.path.basename(ofpath)}")
        req = requests.get(url)
        open(ofpath, 'wb').write(req.content)


if __name__ == "__main__":
    main()