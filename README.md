# SA-1B-Fast-Downloader
The SA-1B Dataset Downloader is a high-performance tool designed to facilitate the efficient and reliable downloading of the SA-1B dataset. Leveraging concurrent processing and robust resume capabilities, this downloader ensures fast and uninterrupted acquisition of large-scale data essential for your projects.

## Installation

1. **Clone the Repository**

    ```bash
    git clone https://github.com/XuyangShen/SA-1B-Fast-Downloader.git
    ```

2. **Install Dependencies**

    Ensure you have Python 3.6 or higher installed. Install the required Python packages using:

    ```bash
    pip install -r requirements.txt
    ```

    **`requirements.txt`**

    ```plaintext
    requests>=2.25.1
    tqdm>=4.60.0
    ```

## Usage

Prepare a TSV file (`download_links.tsv`) containing the target filenames and their corresponding URLs, separated by tabs.

**Example `download_links.tsv`:**

```plaintext
sa_000000.tar	https://example.com/sa_000000.tar
sa_000001.tar	https://example.com/sa_000001.tar
sa_000002.tar	https://example.com/sa_000001.tar
```

Run the downloader with the desired parameters:

```bash
python downloader.py --input download_links.tsv --output raw/ --cpus 10
```

[**Optinal**] Prepare a `retry.txt` file listing any files that failed to download in previous attempts. If `retry.txt` does not exist or is empty, the downloader will attempt to download all `.tar` files listed in `download_links.tsv`.

**Example `retry.txt`:**

```plaintext
sa_000005.tar
sa_000006.tar
```

Run the downloader with the desired parameters:

```bash
python downloader.py --input download_links.tsv --retry retry.txt --output raw/ --cpus 10
```

**Command-Line Arguments:**

- `--input`: Path to the TSV file containing download links (default: `download_links.tsv`).
- `--retry`: Path to the retry downloads file (default: `retry.txt`).
- `--output`: Directory to save downloaded files (default: `raw/`).
- `--cpus`: Number of CPU cores to use for downloading (default: `10`).

## Handling Failed Downloads

After exceeding the maximum number of retries for a file, the downloader records the failed download in `failed_downloads.txt`. Review this file to identify downloads that require manual intervention or further troubleshooting.

## Project Structure

```
sa-1b-fast-downloader/
├── downloader.py
├── download_links.tsv
├── retry.txt [Optional]
├── failed_downloads.txt
├── requirements.txt
├── README.md
├── LICENSE
└── .gitignore
```

- **`downloader.py`**: The main downloader script.
- **`download_links.tsv`**: Example TSV file with download links.
- **`retry.txt`**: File to track retries.
- **`failed_downloads.txt`**: Records downloads that failed after maximum retries.
- **`requirements.txt`**: Lists required Python packages.
- **`README.md`**: Project documentation.
- **`LICENSE`**: Licensing information.
- **`.gitignore`**: Specifies files and directories to ignore in Git.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements, bug fixes, or suggestions.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgements

- Inspired by the need for efficient data acquisition tools in large-scale datasets.
- Utilizes the powerful `requests` and `tqdm` libraries for HTTP requests and progress tracking, respectively.

---
