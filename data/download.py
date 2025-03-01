import os
import aiohttp
import asyncio
import aiofiles
import logging
import pandas as pd
import click
from tqdm.asyncio import tqdm

from config import IMAGES_PATH, LOG_PATH, DATA_PATH
from utils import drop_missing_poster

# Set up logging
os.makedirs(LOG_PATH, exist_ok=True)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s', filename=os.path.join(LOG_PATH, 'download.log'), filemode='a')

# Directory for storing images
os.makedirs(IMAGES_PATH, exist_ok=True)

# Limit the number of concurrent downloads
CONCURRENT_DOWNLOADS = 100

async def download_poster(session, poster_url, movie_id, semaphore):
    """ Asynchronously download a single poster image. """
    poster_path = os.path.join(IMAGES_PATH, f'{movie_id}.jpg')

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
    
@click.command()
@click.argument('csv-file', default='anime.csv')
def main(csv_file):
    df = pd.read_csv(DATA_PATH / csv_file)
    df = drop_missing_poster(df)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(download_posters(df))

if __name__ == "__main__":
    main()
