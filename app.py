import streamlit as st
import pandas as pd
import requests
import concurrent.futures
from typing import Optional, List, Dict

API_URL = "https://gateway.edsn.nl/eancodeboek/v1/ecbinfoset"


def main() -> None:
    """Main function to run the Streamlit app."""
    st.title("EAN Code Retriever")
    st.write(
        "Upload a CSV file containing postal codes and street numbers to retrieve EAN metering data."
    )
    st.write("The results will be displayed and can be downloaded as a CSV file.")

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        validate_and_process_csv(df)


def validate_and_process_csv(df: pd.DataFrame) -> None:
    """Validates the uploaded CSV file and processes it.

    Args:
        df (pd.DataFrame): Dataframe containing the uploaded CSV data.
    """
    required_columns = {"postalCode", "streetNumber"}

    if not required_columns.issubset(df.columns):
        st.error(f"CSV must contain at least the columns: {required_columns}")
        return

    df["postalCode"] = df["postalCode"].astype(str)
    df["streetNumber"] = df["streetNumber"].astype(int)

    metering_data, missing_addresses = process_rows(df)

    if missing_addresses:
        for postal_code, street_number in missing_addresses:
            st.warning(
                f"No ELK or GAS metering points found for postal code {postal_code} and street number {street_number}"
            )

    if metering_data:
        updated_df = pd.DataFrame(metering_data)
        updated_df.sort_values(
            by=["postalCode", "streetNumber", "streetNumberAddition", "product"],
            inplace=True,
        )
        st.write("Metering Data:", updated_df)
        download_csv(updated_df)


def process_rows(df: pd.DataFrame) -> (List[Dict[str, Optional[str]]], List[tuple]):
    """Processes each row in the DataFrame and retrieves metering data concurrently.

    Args:
        df (pd.DataFrame): Dataframe containing validated CSV data.

    Returns:
        list: List of processed metering data records.
        list: List of addresses where no metering points were found.
    """
    metering_data: List[Dict[str, Optional[str]]] = []
    missing_addresses: List[tuple] = []
    tasks = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for _, row in df.iterrows():
            street_number_addition: Optional[str] = row.get("streetNumberAddition")
            street_number_addition = (
                None
                if pd.isna(street_number_addition) or street_number_addition == ""
                else street_number_addition
            )

            address_key = (row["postalCode"], row["streetNumber"])
            tasks[address_key] = []

            for product in ["ELK", "GAS"]:
                tasks[address_key].append(
                    executor.submit(
                        process_product, row, product, street_number_addition
                    )
                )

        for key, futures in tasks.items():
            result_data = []
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    result_data.extend(result)

            if not result_data:
                missing_addresses.append(key)
            else:
                metering_data.extend(result_data)

    return metering_data, missing_addresses


def process_product(
    row: pd.Series, product: str, street_number_addition: Optional[str]
) -> List[Dict[str, Optional[str]]]:
    """Processes a specific product for a given row and returns data.

    Args:
        row (pd.Series): A row from the DataFrame.
        product (str): Product type ("ELK" or "GAS").
        street_number_addition (str, optional): Street number addition.

    Returns:
        list: Processed metering data.
    """
    metering_points = get_metering_points(
        product=product,
        postal_code=row["postalCode"],
        street_number=row["streetNumber"],
        street_number_addition=street_number_addition,
    )

    if metering_points:
        return format_metering_points(row, metering_points)
    else:
        return []


def format_metering_points(
    row: pd.Series, metering_points: List[Dict[str, str]]
) -> List[Dict[str, Optional[str]]]:
    """Formats the retrieved metering points into a structured list.

    Args:
        row (pd.Series): A row from the DataFrame.
        metering_points (list): List of metering point data.

    Returns:
        list: Formatted list of metering point dictionaries.
    """
    return [
        {
            "postalCode": row["postalCode"],
            "streetNumber": row["streetNumber"],
            "streetNumberAddition": meter_point["address"].get("streetNumberAddition"),
            "bagId": meter_point.get("bagId"),
            "product": meter_point["product"],
            "ean": meter_point["ean"],
            "specialMeteringPoint": meter_point["specialMeteringPoint"],
        }
        for meter_point in metering_points
    ]


def get_metering_points(
    product: str,
    postal_code: str,
    street_number: int,
    street_number_addition: Optional[str] = None,
) -> Optional[List[Dict[str, str]]]:
    """Fetches metering points from the API based on address information.

    Args:
        product (str): Product type ("ELK" or "GAS").
        postal_code (str): Postal code of the address.
        street_number (int): Street number of the address.
        street_number_addition (str, optional): Street number addition.

    Returns:
        list or None: List of metering points if found, otherwise None.
    """
    params = {
        "product": product,
        "postalCode": str(postal_code),
        "streetNumber": int(street_number),
    }
    if street_number_addition:
        params["streetNumberAddition"] = str(street_number_addition)

    response = requests.get(API_URL, params=params)
    return (
        response.json().get("meteringPoints", [])
        if response.status_code == 200
        else None
    )


def download_csv(updated_df: pd.DataFrame) -> None:
    """Creates a downloadable CSV file from the processed metering data.

    Args:
        updated_df (pd.DataFrame): DataFrame containing the processed data.
    """
    csv = updated_df.to_csv(index=False)
    st.download_button(
        "Download CSV", data=csv, file_name="metering_data.csv", mime="text/csv"
    )


if __name__ == "__main__":
    main()
