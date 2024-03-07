import zstandard as zstd
import json
import sys
import os
import time
from multiprocessing import Pool

def filter_subreddit_chunk(chunk, target_subreddit):
    filtered_lines = []
    json_decoder = json.JSONDecoder()
    for line in chunk.decode('utf-8').split('\n'):
        if line.strip():
            try:
                reddit_data = json_decoder.raw_decode(line)[0]
                if 'subreddit' in reddit_data and reddit_data['subreddit'] == target_subreddit:
                    filtered_lines.append(line.encode('utf-8') + b'\n')
            except json.JSONDecodeError:
                pass
    return filtered_lines

def process_single_file(input_file, target_subreddit):
    output_filename = f"{os.path.splitext(input_file)[0]}-{target_subreddit}.txt"
    with open(input_file, 'rb') as infile, open(output_filename, 'wb') as outfile:
        decompressor = zstd.ZstdDecompressor(max_window_size=2147483648)
        with decompressor.stream_reader(infile) as reader:
            chunk_size = 2 ** 24  # 16 MB chunks
            pool = Pool()
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                filtered_lines = pool.apply_async(filter_subreddit_chunk, args=(chunk, target_subreddit))
                for line in filtered_lines.get():
                    outfile.write(line)
        pool.close()
        pool.join()

def filter_subreddit(input_path, target_subreddit):
    start_time = time.time()
    if os.path.isdir(input_path):
        for filename in os.listdir(input_path):
            if filename.endswith(".zst"):
                process_single_file(os.path.join(input_path, filename), target_subreddit)
    else:
        if not input_path.endswith(".zst"):
            raise ValueError("Invalid file format. Please provide a .zst file.")
        process_single_file(input_path, target_subreddit)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_path/file> target_subreddit")
        sys.exit(1)
    input_path = sys.argv[1]
    target_subreddit = sys.argv[2]
    filter_subreddit(input_path, target_subreddit)