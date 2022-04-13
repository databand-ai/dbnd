import os
import tarfile
import urllib.request

from os.path import exists


working_dir = os.path.dirname(os.path.realpath(__file__))
data_working_dir = os.path.join(working_dir, "datasets")

DATASET_BUCKET = ""
DATASETS = ["backblaze-data-01gb", "backblaze-data-04gb", "backblaze-data-10gb"]


def download_data():
    if not os.path.exists(data_working_dir):
        os.makedirs(data_working_dir)

    for dataset in DATASETS:
        if not exists(os.path.join(data_working_dir, dataset)):
            print(f"Dataset {dataset} is missing, downloading...")
            file_name = f"{dataset}.tar.gz"
            dataset_url = f"https://{DATASET_BUCKET}/data/backblaze/{file_name}"
            dataset_path = os.path.join(data_working_dir, file_name)
            urllib.request.urlretrieve(dataset_url, dataset_path)
            print("Download complete, extracting")

            opened_tar = tarfile.open(dataset_path)
            opened_tar.extractall(data_working_dir)
            os.remove(dataset_path)
            print(f"Dataset {dataset} downloaded")
