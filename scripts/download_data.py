import kagglehub
import shutil
import os

def download_dataset():
    print("Downloading Olist dataset...")
    # Download latest version
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    
    print("Path to dataset files:", path)
    
    # Move files to our project's 'data' folder so Docker can see them
    target_dir = "./data/raw"
    os.makedirs(target_dir, exist_ok=True)
    
    files = os.listdir(path)
    for file in files:
        if file.endswith(".csv"):
            shutil.copy(os.path.join(path, file), target_dir)
            print(f"Moved {file} to {target_dir}")

if __name__ == "__main__":
    download_dataset()