#!/usr/bin/env python3
"""
repair_dicom_sequences.py

This script recursively scans an input folder for DICOM files and attempts to repair
non-compliant sequence delimitations. Files that trigger warnings (e.g. missing expected
sequence item tags like (FFFE,E000)) are re-read (with warnings suppressed) and then re-saved
using write_like_original=False. This forces pydicom to re-encode sequences with explicit
lengths and proper delimiters per DICOM PS3.5.

Usage:
    python repair_dicom_sequences.py <input_folder> <output_folder>
"""

import os
import sys
import logging
import warnings

import pydicom
from pydicom.errors import InvalidDicomError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def repair_dicom_file(input_filepath, output_filepath):
    """
    Attempt to repair a DICOM file by reading it (suppressing warnings) and then re-saving it
    with standard-compliant encoding (explicit lengths and proper sequence delimiters).
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ds = pydicom.dcmread(input_filepath, force=True)
        ds.save_as(output_filepath, write_like_original=False)
        logging.info(f"Repaired file saved to: {output_filepath}")
    except Exception as e:
        logging.error(f"Failed to repair file {input_filepath}: {e}")

def repair_dicom_directory(input_dir, output_dir):
    """
    Recursively scan input_dir for DICOM files, repair each one, and write to output_dir,
    preserving the directory structure.
    """
    for root, _, files in os.walk(input_dir):
        for file in files:
            input_filepath = os.path.join(root, file)
            # Quick check: try reading a minimal dataset
            try:
                ds = pydicom.dcmread(input_filepath, stop_before_pixels=True, force=True)
            except (InvalidDicomError, Exception):
                continue  # Skip non-DICOM files
            # Build output directory path preserving structure
            rel_path = os.path.relpath(root, input_dir)
            output_root = os.path.join(output_dir, rel_path)
            os.makedirs(output_root, exist_ok=True)
            output_filepath = os.path.join(output_root, file)
            repair_dicom_file(input_filepath, output_filepath)

def main():
    if len(sys.argv) < 3:
        print("Usage: python repair_dicom_sequences.py <input_folder> <output_folder>")
        sys.exit(1)
    input_dir = os.path.abspath(sys.argv[1])
    output_dir = os.path.abspath(sys.argv[2])
    repair_dicom_directory(input_dir, output_dir)

if __name__ == "__main__":
    main()
