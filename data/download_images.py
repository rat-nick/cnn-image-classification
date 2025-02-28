import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import aiohttp
import asyncio
import aiofiles
import logging
import pandas as pd
from tqdm.asyncio import tqdm



# Set up logging
LOG_DIR = './logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s', filename=os.path.join(LOG_DIR, 'download.log'), filemode='a')

# Directory for storing images
IMAGE_DIR = './images'
os.makedirs(IMAGE_DIR, exist_ok=True)

# Limit the number of concurrent downloads
CONCURRENT_DOWNLOADS = 100

async def download_poster(session, poster_url, movie_id, semaphore):
    """ Asynchronously download a single poster image. """
    poster_path = os.path.join(IMAGE_DIR, f'{movie_id}.jpg')

    async with semaphore:
        try:
            async with session.get(poster_url) as response:
                if response.status == 200:
                    async with aiofiles.open(poster_path, 'wb') as f:
                        await f.write(await response.read())
                    return poster_path
                else:
                    raise Exception(f"Failed to download {poster_url} - Status {response.status}")
        except Exception as e:
            logging.error(f"Error downloading {poster_url} for movie_id {movie_id}: {e}")
            return None

async def download_posters(df):
    """ Asynchronously download all posters with progress tracking. """
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            poster_url = row['poster_url']
            movie_id = row['malID']
            tasks.append(download_poster(session, poster_url, movie_id, semaphore))

        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading posters", unit="img"):
            results.append(await f)

    return results

def main():
    df = pd.read_csv('anime.csv')
    from utils import drop_missing_poster
    df = drop_missing_poster(df)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(download_posters(df))
    

if __name__ == "__main__":
    main()