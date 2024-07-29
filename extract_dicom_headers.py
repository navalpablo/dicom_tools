import os
import argparse
import pydicom
import csv
from collections import OrderedDict
from tqdm import tqdm

def find_dicom_files(directory, read_all):
    for root, dirs, files in os.walk(directory):
        dicom_files = [os.path.join(root, f) for f in files if f.endswith('.dcm') or '.' not in f]
        if not read_all:
            dicom_files = dicom_files[:5]  # Limit to first 5 files if not reading all
        for file_path in dicom_files:
            yield file_path

def hex_string_to_tag(hex_str):
    group, element = hex_str[:4], hex_str[4:]
    return (int(group, 16), int(element, 16))

def extract_dicom_info(file_path, fields):
    try:
        dicom = pydicom.dcmread(file_path, stop_before_pixels=True)
        info = OrderedDict()
        for field in fields:
            tag = hex_string_to_tag(field)
            if tag in dicom:
                value = str(dicom[tag].value)
                info[field] = value
            else:
                info[field] = ''
        return info
    except Exception as e:
        print(f"Error reading DICOM file {file_path}: {e}")
        return None

def main(dicom_dir, output_path, read_all=False):
    # Define a mapping from DICOM tags to descriptive names
    dicom_field_mapping = {
        "00100020": "Patient ID",
        "00080020": "Study Date",
        "00180087": "Magnetic Field Strength",
        "00081090": "Manufacturer's Model Name",
        "00080080": "Institution Name",
        "00080050": "Accession Number",
        "0020000D": "Study Instance UID",
        "00200011": "Series Number",
        "0008103E": "Series Description",
        "0020000E": "Series Instance UID",
        "00540081": "Number of Slices",
        "00181310": "Acquisition Matrix",
        "00280030": "Pixel Spacing",
        "00180088": "Spacing Between Slices",
        "00180050": "Slice Thickness",
        "00180080": "Repetition Time",
        "00180081": "Echo Time",
        "00180086": "Echo Number(s)",
        "00180091": "Echo Train Length",
        "00180082": "Inversion Time",
        "00181314": "Flip Angle",
        "00080008": "Image Type",
        "00189073": "Acquisition Duration",  # Added Acquisition Duration
        "2001101B": "Prepulse Delay",  # Added Prepulse Delay
    }

    # The fields to extract, in order
    fields = list(dicom_field_mapping.keys())

    # Header row with descriptive names for each field
    header_row = list(dicom_field_mapping.values())

    unique_sequences = {}
    all_files = list(find_dicom_files(dicom_dir, read_all))  # Pass the 'read_all' parameter
    
    for file_path in tqdm(all_files, desc="Processing DICOM files"):
        info = extract_dicom_info(file_path, fields)
        if info:
            # Combine Study Instance UID and Series Number to create a unique ID
            unique_id = (info.get("0020000D", ""), info.get("0008103E", ""))
            if unique_id not in unique_sequences and all(unique_id):
                unique_sequences[unique_id] = info

    # When writing the TSV file
    with open(output_path, 'w', newline='') as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=fields, delimiter='\t', extrasaction='ignore')
        # Write the header row with descriptive names
        writer.writerow(dict(zip(fields, header_row)))
        # Now write the data rows
        for sequence in unique_sequences.values():
            writer.writerow(sequence)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and save unique DICOM sequence information to a CSV file. This script allows for the extraction of specific DICOM header fields from files in a given directory.",
        epilog="""Examples of use:
        
        # Basic usage to process first 5 DICOM files in a directory and save output:
        python extract_dicom_headers.py --dicom test --output out_test.tsv
        
        # Process all DICOM files in the directory:
        python extract_dicom_headers.py --dicom test --output out_test.tsv --read_all""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Add arguments
    parser.add_argument("--dicom", required=True, help="Path to the directory containing DICOM files.")
    parser.add_argument("--output", required=True, help="Path to save the output CSV file.")
    parser.add_argument("--read_all", action='store_true', help="Read all DICOM files in the directory, not just the first 5.")

    # Parse the arguments
    args = parser.parse_args()

    main(args.dicom, args.output, args.read_all)
