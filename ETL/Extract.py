import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_file_with_kaggle_api(dataset_name, download_dir):
    """downloading and unzipping data set from kaggle"""

    os.makedirs(download_dir, exist_ok = True)
    #storing the api instance for further use later
    api = KaggleApi()
    #Authentication using KaggleApi class
    api.authenticate()

    print(f"[INFO] downloading dataset of {dataset_name}")
    api.dataset_download_files(dataset_name, path=download_dir, unzip=True)
    print(f"[SUCCESS] dataset has been downloaded and extracted to: {download_dir}")


if __name__ == "__main__":
    dataset = "joyshil0599/mlb-hitting-and-pitching-stats-through-the-years"
    
    #using the script's folder as base directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(base_dir, "data", "raw")
    download_file_with_kaggle_api(dataset, target_dir)

