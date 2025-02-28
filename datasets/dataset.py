import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import pandas as pd
from PIL import Image
from torch.utils.data import Dataset
from torchvision import transforms as T
from utils import one_hot_encode_genres
class ImageClassificationDataset(Dataset):
    def __init__(self, image_dir, csv_file, transform=T.ToTensor()):
        self.inner_ids = [f[:-4] for f in os.listdir(image_dir) if f.endswith('.jpg')]
        self.n = len(self.inner_ids)
        self.image_dir = image_dir
        self.data_info = one_hot_encode_genres(pd.read_csv(csv_file))
        self.transform = transform

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        id = self.inner_ids[idx]
        img_name = os.path.join(self.image_dir, id + '.jpg')
        row = self.data_info[self.data_info['malID'] == int(id)].iloc[0]
        label = int(row['comedy'])
        image = Image.open(img_name).convert("RGB")
        

        if self.transform:
            image = self.transform(image)

        return image, label
