import streamlit as st
import pandas as pd
import requests
import concurrent.futures
from typing import Optional, List, Dict

API_URL = "https://gateway.edsn.nl/eancodeboek/v1/ecbinfoset"


def main() -> None:
    """Main function to run the Streamlit app."""
    st.title("EAN Code Retriever")
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

    metering_data = process_rows(df)

    if metering_data:
        updated_df = pd.DataFrame(metering_data)
        updated_df.sort_values(
            by=["postalCode", "streetNumber", "streetNumberAddition", "product"],
            inplace=True,
        )
        st.write("Metering Data:", updated_df)
        download_csv(updated_df)


def process_rows(df: pd.DataFrame) -> List[Dict[str, Optional[str]]]:
    """Processes each row in the DataFrame and retrieves metering data concurrently.

    Args:
        df (pd.DataFrame): Dataframe containing validated CSV data.

    Returns:
        list: List of processed metering data records.
    """
    metering_data: List[Dict[str, Optional[str]]] = []
    tasks = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for _, row in df.iterrows():
            street_number_addition: Optional[str] = row.get("streetNumberAddition")
            street_number_addition = (
                None
                if pd.isna(street_number_addition) or street_number_addition == ""
                else street_number_addition
            )

            for product in ["ELK", "GAS"]:
                tasks.append(
                    executor.submit(
                        process_product, row, product, street_number_addition
                    )
                )

        for future in concurrent.futures.as_completed(tasks):
            result = future.result()
            if result:
                metering_data.extend(result)

    return metering_data


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
        row_dict = row.to_dict()
        row_dict["product"] = product  # Ensure product is explicitly stated
        row_dict["ean"] = None  # Explicitly show no EAN available
        return [row_dict]


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
