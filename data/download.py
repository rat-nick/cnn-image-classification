import os
import json
import asyncio
import logging
import aiohttp
import aiofiles
import async_timeout
import pandas as pd
import click
from tqdm.asyncio import tqdm
from pathlib import Path
from aiohttp_retry import RetryClient, ExponentialRetry
from contextlib import asynccontextmanager

from config import IMAGES_PATH, LOG_PATH, DATA_PATH
from utils import drop_missing_poster

# Ensure required directories exist
LOG_PATH.mkdir(parents=True, exist_ok=True)
IMAGES_PATH.mkdir(parents=True, exist_ok=True)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_PATH / 'download.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

def log_error(message, **kwargs):
    """ Log errors in structured JSON format. """
    log_data = {"message": message, **kwargs}
    logger.error(json.dumps(log_data))

# Limit concurrent downloads
CONCURRENT_DOWNLOADS = 100
REQUEST_TIMEOUT = 10  # Timeout in seconds
MAX_RETRIES = 3  # Maximum retry attempts

@asynccontextmanager
async def controlled_session():
    """ Provides a controlled session with a semaphore to manage concurrency. """
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    async with aiohttp.ClientSession() as session:
        yield session, semaphore

async def fetch_poster(session, poster_url):
    """ Fetch the poster image from the URL. """
    retry_options = ExponentialRetry(attempts=MAX_RETRIES)
    retry_session = RetryClient(session, retry_options=retry_options)
    async with async_timeout.timeout(REQUEST_TIMEOUT):
        async with retry_session.get(poster_url) as response:
            if response.status == 200:
                return await response.read()
            else:
                log_error("Failed to download", poster_url=poster_url, status=response.status)
                return None

async def save_poster(poster_data, poster_path, movie_id):
    """ Save the poster image to the file system. """
    try:
        async with aiofiles.open(poster_path, 'wb') as f:
            await f.write(poster_data)
        return poster_path
    except OSError as e:
        log_error("File write error", movie_id=movie_id, path=str(poster_path), error=str(e))
        return None

async def download_poster(session, poster_url, movie_id, semaphore):
    """ Asynchronously download a single poster image with retries and timeouts. """
    poster_path = Path(IMAGES_PATH) / f"{movie_id}.jpg"
    async with semaphore:
        try:
            poster_data = await fetch_poster(session, poster_url)
            if poster_data:
                return await save_poster(poster_data, poster_path, movie_id)
        except asyncio.TimeoutError:
            log_error("Download timeout", poster_url=poster_url, movie_id=movie_id)
        except Exception as e:
            log_error("Exception during download", poster_url=poster_url, movie_id=movie_id, error=str(e))
        return None

async def download_posters(df):
    """ Asynchronously download all posters with improved concurrency handling. """
    async with controlled_session() as (session, semaphore):
        tasks = [download_poster(session, row['poster_url'], row['malID'], semaphore) for _, row in df.iterrows()]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading posters", unit="img"):
            await task

@click.command()
@click.argument('csv-file', default='anime.csv')
def main(csv_file):
    """ Main function to process CSV file and start asynchronous downloading. """
    df = pd.read_csv(Path(DATA_PATH) / csv_file)
    df = drop_missing_poster(df)
    
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(download_posters(df))

if __name__ == "__main__":
    main()
