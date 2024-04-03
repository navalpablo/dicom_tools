# dicom_tools

A collection of Python scripts designed to facilitate the handling and analysis of DICOM files, specifically tailored for decompressing DICOM files, extracting DICOM headers for database analysis, and generating dummy tables for sequence identification.

## Scripts Included

### 1. decompress.py

This script is used to recursively decompress all DICOM files within a specified folder while maintaining the original folder structure.

#### Usage

    python decompress.py <path_to_directory>

#### Requirements

- Python 3.x
- The `dcmdjpeg` tool from DCMTK must be installed and accessible in your system's PATH.

### 2. extract_dicom_headers.py

Extracts specific DICOM headers from each sequence available in the database, facilitating the listing of sequence characteristics. Additionally, it can generate a dummy table, with a row per subject, indicating the presence or absence of a sequence by series description.

#### Usage

    python extract_dicom_headers.py --dicom <dicom_directory> --output <output_path.tsv> [--read_all] [--dummy_table <series_descriptions>...]

- `--dicom`: Directory containing DICOM files.
- `--output`: Output path for the TSV file.
- `--read_all`: Optional flag to process all DICOM files instead of just the first 5.
- `--dummy_table`: Optional parameter to specify series descriptions for dummy table generation.

#### Requirements

- Python 3.x
- pydicom
- tqdm

### 3. create_dummy.py

Generates a dummy table from an existing TSV file containing DICOM sequence information, supporting complex OR conditions specified in a JSON format.

#### Usage

    python create_dummy.py --input <input_tsv_path> --criteria_json <'{"SeriesDesc1": ["cond1", "cond2"], "SeriesDesc2": "cond3"}'>

- `--input`: Path to the input TSV file.
- `--criteria_json`: JSON string specifying the series descriptions and any OR conditions.
