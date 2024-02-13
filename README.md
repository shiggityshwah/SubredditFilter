# Subreddit Filter

This Python script leverages the Zstandard (Zstd) compression library and the Reddit API data format to filter posts from a given subreddit. It takes an input file, which is assumed to be compressed with Zstandard, and extracts posts related to a specified subreddit. The filtered posts are then written to an output file.

## Usage

To run the script, use the following command:

```bash
python filter.py input_file target_subreddit
OR -
python filter.py input_directory target_subreddit
```
input_file: The path to the Zstandard compressed input file.
input_directory: The path to the directory containing `.zst` files.
target_subreddit: The subreddit name to filter posts from.
Dependencies
Make sure to install the required dependencies using the following:

```bash
pip install zstandard
```

## Notes
The script utilizes Zstandard decompression and JSON decoding to process the input file.
Adjust the chunk size and other parameters as needed for specific use cases.
Execution time is displayed after completion.
Feel free to customize the script for your specific needs, and refer to the comments within the code for further details on its functionality.
