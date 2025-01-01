# Consulting Data from Sankhya API using Python

This repository contains Python scripts that consult data from the Sankhya Service API and save it in CSV files.

## Requirements

To install the required dependencies, run:

```bash
pip install -r requirements.txt
```

## Project Structure

```
.
├── .gitignore
├── README.md
├── requirements.txt
├── SankhyaExtractFromDateAlter.py
└── SankhyaExtractFromDateFatur.py
```

- **.gitignore**: Specifies files and directories to be ignored by git.
- **README.md**: This file, containing information about the project.
- **requirements.txt**: Lists the dependencies required for the project.
- **SankhyaExtractFromDateAlter.py**: Script to extract data based on the `DTALTER` field.
- **SankhyaExtractFromDateFatur.py**: Script to extract data based on the `DTFATUR` field.

## Usage

1. **Set up environment variables**:
   Create a `.env` file in the root directory of the project and add the following variables:

   ```env
   SANKHYA_USER=your_username
   SANKHYA_PASSWORD=your_password
   SANKHYA_HOST=your_host
   SANKHYA_PORT=your_port
   ```

2. **Run the scripts**:

   - To run the `SankhyaExtractFromDateAlter.py` script:

     ```bash
     python SankhyaExtractFromDateAlter.py
     ```

   - To run the `SankhyaExtractFromDateFatur.py` script:

     ```bash
     python SankhyaExtractFromDateFatur.py
     ```

## Logging

Both scripts use Python's `logging` module to log information, warnings, and errors. The logs are displayed in the console.