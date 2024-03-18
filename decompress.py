import os
import subprocess
import argparse

def decompress_file(file_path):
    try:
        # Attempt to decompress the file, overwriting the original
        subprocess.run(["dcmdjpeg", file_path, file_path], check=True)
        print(f"Processed: {file_path}")
    except subprocess.CalledProcessError as e:
        # This error is raised for non-DICOM files or other failures
        print(f"Skipping (not a compressed DICOM or other error): {file_path}")

def walk_and_process(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            decompress_file(full_path)

def main():
    parser = argparse.ArgumentParser(description='Recursively decompress DICOM files in a directory tree.')
    parser.add_argument('path', type=str, help='Path to the directory to process.')
    args = parser.parse_args()

    # Change to specified directory
    os.chdir(args.path)
    print(f"Current working directory set to: {os.getcwd()}")

    # Start processing
    walk_and_process(os.getcwd())

if __name__ == "__main__":
    main()
