import os
import pydicom
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse

def get_dicom_attributes(dicom_file):
    """Get relevant DICOM attributes to check for duplicates."""
    ds = pydicom.dcmread(dicom_file)
    attributes = {
        "SeriesDescription": ds.SeriesDescription,
        "SeriesNumber": ds.SeriesNumber,
        "PatientID": ds.PatientID,
        "StudyDate": ds.StudyDate,
        "SliceLocation": ds.SliceLocation
    }
    return attributes

def process_file(file_path):
    """Process a single DICOM file and return its attributes and path."""
    try:
        attributes = get_dicom_attributes(file_path)
        identifier = tuple(attributes.values())
        return identifier, file_path
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def find_duplicates(root_folder):
    dicom_identifiers = defaultdict(list)
    total_files = sum([len(files) for _, _, files in os.walk(root_folder) if any(file.lower().endswith('.dcm') for file in files)])

    # Collect all DICOM file paths
    dicom_files = []
    for subdir, _, files in os.walk(root_folder):
        for file in files:
            if file.lower().endswith('.dcm'):
                file_path = os.path.join(subdir, file)
                dicom_files.append(file_path)

    with tqdm(total=total_files, desc="Processing DICOM files") as pbar:
        with ProcessPoolExecutor() as executor:
            future_to_file = {executor.submit(process_file, file_path): file_path for file_path in dicom_files}

            for future in as_completed(future_to_file):
                result = future.result()
                if result:
                    identifier, file_path = result
                    dicom_identifiers[identifier].append(file_path)
                pbar.update(1)

    duplicates = {k: v for k, v in dicom_identifiers.items() if len(v) > 1}
    return duplicates

def map_duplicates(duplicates):
    for attributes, file_paths in duplicates.items():
        print(f"Found duplicates for attributes {attributes}:")
        for file_path in file_paths:
            print(f"  {file_path}")

def confirm_and_delete(duplicates):
    while True:
        delete = input("Duplicates found. Do you want to delete the duplicates? (yes/no): ").strip().lower()
        if delete in ['yes', 'no']:
            break

    if delete == 'yes':
        for attributes, file_paths in duplicates.items():
            # Keep the first file, delete the rest
            for file_path in file_paths[1:]:
                print(f"Removing duplicate file: {file_path}")
                os.remove(file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and remove duplicate DICOM files based on specific attributes.")
    parser.add_argument("root_folder", type=str, help="Path to the root folder containing DICOM files.")

    args = parser.parse_args()
    root_folder = args.root_folder

    duplicates = find_duplicates(root_folder)
    if duplicates:
        map_duplicates(duplicates)
        confirm_and_delete(duplicates)
    else:
        print("No duplicates found.")
