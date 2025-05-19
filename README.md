# EAN Code Retriever

EAN Code Retriever is a user-friendly application for retrieving European Article Numbering (EAN) codes for electricity (ELK) and gas (GAS) connections in the Netherlands. It uses Streamlit for an interactive interface and efficiently processes CSV file inputs to acquire accurate metering point data.

You can access the app publicly via the Streamlit Community Cloud here: https://ean-code-retriever.streamlit.app/.

## Features

- **Upload CSV file** – Easily upload CSV files containing postal codes and street numbers to retrieve EAN codes.
- **Data validation** – Automatically checks input data to ensure necessary fields are present and correctly formatted.
- **API integration** – Connects with the EDSN API to retrieve metering point information based on provided addresses.
- **Display results** – Shows retrieved EAN codes and associated details directly within the application.
- **Download metering data** – Provides an option to download the metering data as a CSV file.

## Requirements

- Python 3.x
- Streamlit
- Pandas
- Requests

## Installation

To set up and run the application:

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Start the Streamlit application:

   ```bash
   streamlit run app.py
   ```

## Usage

1. Open the Streamlit application in your web browser.
2. Upload a CSV file using the file uploader. Ensure your file contains at least the columns `postalCode` and `streetNumber`.
3. The application processes the CSV and retrieves the corresponding EAN codes.
4. The metering data, including the retrieved EAN codes, is displayed in the application.
5. Download the metering data as a CSV file.

## CSV file format

Your input CSV file should have the following structure:

- `postalCode` – The postal code where the metering connection is located.
- `streetNumber` – The street number for the connection point.
- Optional: `streetNumberAddition` – Any additional identifiers for the street number.

An example input CSV file is included as `example.csv`.

## Contributing

Contributions are welcome! Feel free to open issues, submit pull requests, or suggest features.

---

This application enhances accessibility and accuracy for professionals in the energy sector who need to identify and verify metering connections in the Netherlands.
