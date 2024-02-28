import concurrent.futures
import zstandard as zstd
import json
import sys
import os
import time


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


def process_single_file(input_file, target_subreddit):
    output_filename = f"{os.path.splitext(input_file)[0]}-{target_subreddit}.zst"

    with open(input_file, 'rb') as infile, zstd.open(output_filename, 'wb') as outfile:
        decompressor = zstd.ZstdDecompressor(max_window_size=2147483648)
        json_decoder = json.JSONDecoder()

        with decompressor.stream_reader(infile) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(2**24)  # 16mb chunks
                if not chunk:
                    break

                string_data = chunk.decode('utf-8')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line

                    try:
                        reddit_data = json_decoder.raw_decode(line)[0]

                        if 'subreddit' in reddit_data and reddit_data['subreddit'] == target_subreddit:
                            outfile.write(line.encode('utf-8'))  # Write directly to zstd file
                            outfile.write(b'\n')  # Add newline for readability
                    except json.JSONDecodeError:
                        pass

                previous_line = lines[-1]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_path/file> target_subreddit")
        sys.exit(1)

    input_path = sys.argv[1]
    target_subreddit = sys.argv[2]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.submit(filter_subreddit, input_path, target_subreddit)
