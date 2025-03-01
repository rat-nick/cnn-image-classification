import yaml
from pathlib import Path

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

values = load_config()

BASE_PATH = Path(values['base_dir']).resolve()
DATA_PATH = BASE_PATH / values['data_dir']
OUTPUT_PATH = BASE_PATH / values['output_dir']
LOG_PATH = BASE_PATH / values['log_dir']
MODEL_PATH = OUTPUT_PATH / values['model_dir']
IMAGES_PATH = DATA_PATH / values['images_dir']

def display_config():
    print("Configuration:")
    print(f"BASE_PATH:\t {BASE_PATH}")
    print(f"DATA_PATH:\t {DATA_PATH}")
    print(f"OUTPUT_PATH:\t {OUTPUT_PATH}")
    print(f"LOG_PATH:\t {LOG_PATH}")
    print(f"MODEL_PATH:\t {MODEL_PATH}")
    print(f"IMAGES_PATH:\t {IMAGES_PATH}")

if __name__ == "__main__":
    display_config()