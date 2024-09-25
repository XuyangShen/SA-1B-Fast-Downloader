#!/usr/bin/env python3
"""
Concurrent SA-1B File Downloader

This script downloads `.tar` files concurrently using multiprocessing.
It supports resuming partial downloads and provides progress bars for each download.

Usage:
    python downloader.py --input download_link.tsv --retry retry.txt --output raw/ --cpus 10
"""

import argparse
from functools import partial
import multiprocessing as mp
import os
import sys
import time
from typing import Dict, Generator, Tuple

import requests
from tqdm import tqdm


def download_file(url: str, local_filename: str) -> None:
    """
    Downloads a `.tar` file from a given URL to a local path with support for resuming.

    Args:
        url (str): The URL to download from.
        local_filename (str): The local file path where the file will be saved.

    Raises:
        Exception: If the download fails with an unexpected status code.
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)

        # Check if the file already exists
        current_size = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0

        headers = {}
        if current_size > 0:
            headers['Range'] = f'bytes={current_size}-'

        # Make the HTTP request with streaming enabled
        with requests.get(url, headers=headers, stream=True, timeout=30) as response:
            if response.status_code in (200, 206):
                mode = 'ab' if current_size > 0 else 'wb'

                # Get the total file size from headers
                total_size = response.headers.get('Content-Length')
                if total_size is not None:
                    total_size = int(total_size) + current_size
                else:
                    total_size = None  # Unknown size

                # Initialize the tqdm progress bar
                desc = os.path.basename(local_filename)
                with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        initial=current_size,
                        desc=desc,
                        ascii=True,
                        position=int(os.getpid() % 10)  # To prevent overlap in multiprocessing
                ) as progress_bar:
                    with open(local_filename, mode) as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep-alive chunks
                                f.write(chunk)
                                progress_bar.update(len(chunk))

                current_size = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0
                if total_size is not None:
                    if total_size <= current_size:
                        print(f"Download completed: {local_filename}")
                    else:
                        raise Exception(f'Unexcepted exit. current size: {current_size}, total size: {total_size}')
            elif response.status_code == 416:
                print(f"Download already completed: {local_filename}")
            else:
                raise Exception(f"Failed to download file. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        raise


def load_tsv(path: str) -> Dict[str, str]:
    """
    Loads a TSV file mapping target `.tar` filenames to URLs.

    Args:
        path (str): Path to the TSV file.

    Returns:
        Dict[str, str]: A dictionary mapping target filenames to URLs.
    """
    map2url = {}
    try:
        with open(path, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    if parts[0].endswith('.tar'):
                        map2url[parts[0]] = parts[1]
                else:
                    print(f"Invalid line in TSV: {line.strip()}")
    except FileNotFoundError:
        print(f"TSV file not found: {path}")
        sys.exit(1)
    return map2url


def iterate_retry(retry_path: str, map2url: Dict[str, str]) -> Generator[Tuple[str, str], None, None]:
    """
    Generator that yields tuples of (target, url) for failed or pending downloads.

    Args:
        retry_path (str): Path to the retry.txt file.
        map2url (Dict[str, str]): Mapping of target filenames to URLs.

    Yields:
        Tuple[str, str]: (target, url)
    """
    to_downloads = []

    if not os.path.exists(retry_path):
        # No retry file; attempt to download all `.tar` files
        to_downloads = [tar for tar in sorted(map2url.keys()) if tar.endswith('.tar')]
        print('No retry file found. Attempting to download all `.tar` files.')
    else:
        with open(retry_path, 'r') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines if line.strip()]

        if not lines:
            # Retry file is empty; attempt to download all `.tar` files
            to_downloads = [tar for tar in sorted(map2url.keys()) if tar.endswith('.tar')]
            print('Retry file is empty. Attempting to download all `.tar` files.')
        else:
            to_downloads = lines
            print(f"Found {len(to_downloads)} files in retry file to download.")

    for tar in to_downloads:
        url = map2url.get(tar)
        if url:
            yield tar, url
        else:
            print(f'No URL found for target: {tar}')
            # Optionally, write to a separate failed file or handle accordingly
            continue


def download_task(args: Tuple[str, str], output_dir: str, max_retries: int = 10, backoff_factor: float = 0.5) -> None:
    """
    Wrapper function for downloading a single file with retries.

    Args:
        args (Tuple[str, str]): Tuple containing (target, url).
        output_dir (str): Directory where files will be saved.
        max_retries (int): Maximum number of retries before giving up (default: 5).
        backoff_factor (float): Factor for exponential backoff in seconds (default: 0.5).
    """
    tar, url = args
    local_path = os.path.join(output_dir, tar)
    print(f"Starting download: {tar} from {url}")

    retries = 0
    while retries <= max_retries:
        try:
            download_file(url, local_path)
            break  # Exit loop if download is successful
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print(f"Exceeded maximum retries for {tar}. Skipping.")
                # Optionally, write to a separate failed file
                with open('failed_downloads.txt', 'a') as failed_file:
                    failed_file.write(f"{tar}\n")
                break
            sleep_time = backoff_factor * (2**(retries - 1))
            print(f"Retry {retries}/{max_retries} for {tar} after {sleep_time} seconds due to error: {e}")
            time.sleep(sleep_time)


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Concurrent File Downloader")
    parser.add_argument('--input',
                        type=str,
                        default='download_link.tsv',
                        help='Path to the TSV file containing download links (default: download_links.tsv)')
    parser.add_argument('--retry',
                        type=str,
                        default='retry.txt',
                        help='Path to the retry downloads file (default: retry.txt)')
    parser.add_argument('--output', type=str, default='raw/', help='Directory to save downloaded files (default: raw/)')
    parser.add_argument('--cpus', type=int, default=10, help='Number of CPU cores to use for downloading (default: 10)')
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    # Load mappings
    map2url = load_tsv(args.input)

    # Create a partial function to include output_dir, max_retries, and backoff_factor
    download_partial = partial(download_task, output_dir=args.output, max_retries=10, backoff_factor=0.5)

    # Prepare tasks
    tasks = list(iterate_retry(args.retry, map2url))
    total_tasks = len(tasks)

    if total_tasks == 0:
        print("No files to download. Exiting.")
        sys.exit(0)

    # Initialize multiprocessing pool
    with mp.Pool(processes=args.cpus) as pool:
        try:
            # Use tqdm to monitor overall progress
            for _ in tqdm(pool.imap_unordered(download_partial, tasks),
                          total=total_tasks,
                          desc="Downloading",
                          unit="file"):
                pass
        except KeyboardInterrupt:
            print("Download interrupted by user.")
            pool.terminate()
            pool.join()
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            pool.terminate()
            pool.join()
            sys.exit(1)


if __name__ == '__main__':
    main()
