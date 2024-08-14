import os
import os
import csv

def split_csv(input_folder, output_folder, max_size=0.25 * 1024 * 1024, target_percentage=1.0):
    """
    Splits large CSV files into smaller chunks.

    Args:
        input_folder: The folder containing the large CSV files.
        output_folder: The folder where split files will be saved.
        max_size: The maximum size (in bytes) for each output file (default: 100MB).
        target_percentage: Percentage of max_size to aim for (default: 95%, i.e., 95MB for 100MB max_size).
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    target_size = max_size * target_percentage

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_file_path = os.path.join(input_folder, filename)
            output_base_name = os.path.splitext(filename)[0]
            output_file_count = 0

            with open(input_file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                try:
                    header = next(reader)  
                except StopIteration:
                    print(f"Skipping empty file: {filename}")
                    continue

                current_size = 0
                outfile = None

                for row in reader:
                    if outfile is None or current_size > target_size:  # Check against target_size
                        output_file_count += 1
                        if outfile:
                            outfile.close()
                        output_file_name = f"{output_base_name}_{output_file_count}.csv"
                        output_file_path = os.path.join(output_folder, output_file_name)
                        outfile = open(output_file_path, 'w', newline='', encoding='utf-8')
                        writer = csv.writer(outfile)
                        writer.writerow(header)
                        current_size = 0

                    writer.writerow(row)
                    current_size += len(','.join(row)) + 1

                if outfile:
                    outfile.close()


if __name__ == "__main__":
    input_folder = "large-files"
    input_folder = os.path.join(input_folder, "retries")
    output_folder = os.path.join(input_folder, "outbox")
    split_csv(input_folder, output_folder, target_percentage=0.9)  # Aim for 90% of max_size
