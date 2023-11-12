import zstandard as zstd
import json
import sys
import time

def filter_subreddit(input_file, output_file, target_subreddit):
    start_time = time.time()

    with open(input_file, 'rb') as infile, open(output_file, 'wb') as outfile:
        # Create a Zstandard decompressor
        decompressor = zstd.ZstdDecompressor(max_window_size=2147483648)

        # Create a JSON decoder
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
                        # Attempt to decode the JSON line
                        reddit_data = json_decoder.raw_decode(line)[0]
                        
                        # Check if the post is from the target subreddit
                        if 'subreddit' in reddit_data and reddit_data['subreddit'] == target_subreddit:
                            # If from the target subreddit, write the JSON object to the output file
                            outfile.write(line.encode('utf-8') + b'\n')
                    except json.JSONDecodeError:
                        # Handle JSON decoding errors (e.g., incomplete lines)
                        pass

                previous_line = lines[-1]

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py input_file output_file target_subreddit")
        sys.exit(1)

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    target_subreddit = sys.argv[3]

    filter_subreddit(input_filename, output_filename, target_subreddit)
