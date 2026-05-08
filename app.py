import concurrent.futures
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

API_URL = "https://gateway.edsn.nl/eancodeboek/v1/ecbinfoset"
PRODUCTS = ["ELK", "GAS"]
MAX_WORKERS = 10
REQUEST_TIMEOUT_SECONDS = 10

MeteringRecord = Dict[str, Any]
AddressKey = Tuple[str, int, Optional[str]]


def main() -> None:
    """Run the Streamlit app."""
    st.title("EAN Code Retriever")

    st.write(
        "Upload a CSV file containing postal codes and street numbers to retrieve EAN metering data."
    )
    st.write("The results will be displayed and can be downloaded as a CSV file.")

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
        except Exception as error:
            st.error(f"Could not read CSV file: {error}")
            return

        validate_and_process_csv(df)


def validate_and_process_csv(df: pd.DataFrame) -> None:
    """Validate and process the uploaded CSV file."""
    required_columns = {"postalCode", "streetNumber"}

    if not required_columns.issubset(df.columns):
        st.error(f"CSV must contain at least the columns: {sorted(required_columns)}")
        return

    df = df.copy()

    df["postalCode"] = (
        df["postalCode"]
        .astype(str)
        .str.upper()
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    df["streetNumber"] = pd.to_numeric(df["streetNumber"], errors="coerce")

    invalid_rows = df[df["streetNumber"].isna()]
    if not invalid_rows.empty:
        st.error("Some streetNumber values are invalid or missing.")
        st.dataframe(invalid_rows)
        return

    df["streetNumber"] = df["streetNumber"].astype(int)

    if "streetNumberAddition" in df.columns:
        df["streetNumberAddition"] = (
            df["streetNumberAddition"].astype("string").str.strip().replace({"": pd.NA})
        )
    else:
        df["streetNumberAddition"] = pd.NA

    metering_data, missing_addresses = process_rows(df)

    if missing_addresses:
        for postal_code, street_number, street_number_addition in missing_addresses:
            addition_text = (
                f" {street_number_addition}" if street_number_addition else ""
            )
            st.warning(
                f"No ELK or GAS metering points found for postal code "
                f"{postal_code} and street number {street_number}{addition_text}"
            )

    if not metering_data:
        st.info("No metering data found.")
        return

    updated_df = pd.DataFrame(metering_data)

    updated_df.sort_values(
        by=["postalCode", "streetNumber", "streetNumberAddition", "product"],
        inplace=True,
    )
    updated_df.reset_index(drop=True, inplace=True)

    st.subheader("Metering Data")
    st.dataframe(updated_df, use_container_width=True)

    download_csv(updated_df)


def process_rows(
    df: pd.DataFrame,
) -> Tuple[List[MeteringRecord], List[AddressKey]]:
    """Process each row and retrieve metering data concurrently."""
    metering_data: List[MeteringRecord] = []
    missing_addresses: List[AddressKey] = []

    tasks = []

    progress_bar = st.progress(0)
    total_addresses = len(df)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _, row in df.iterrows():
            street_number_addition = normalize_optional_value(
                row.get("streetNumberAddition")
            )

            address_key: AddressKey = (
                row["postalCode"],
                row["streetNumber"],
                street_number_addition,
            )

            futures = [
                executor.submit(
                    process_product,
                    row,
                    product,
                    street_number_addition,
                )
                for product in PRODUCTS
            ]

            tasks.append(
                {
                    "address": address_key,
                    "futures": futures,
                }
            )

        completed_addresses = 0

        for task in tasks:
            result_data: List[MeteringRecord] = []

            for future in concurrent.futures.as_completed(task["futures"]):
                try:
                    result = future.result()
                except Exception as error:
                    st.warning(f"Could not process one request: {error}")
                    result = []

                if result:
                    result_data.extend(result)

            if result_data:
                metering_data.extend(result_data)
            else:
                missing_addresses.append(task["address"])

            completed_addresses += 1
            progress_bar.progress(completed_addresses / total_addresses)

    return metering_data, missing_addresses


def process_product(
    row: pd.Series,
    product: str,
    street_number_addition: Optional[str],
) -> List[MeteringRecord]:
    """Process one product type for a given address."""
    metering_points = get_metering_points(
        product=product,
        postal_code=row["postalCode"],
        street_number=row["streetNumber"],
        street_number_addition=street_number_addition,
    )

    if not metering_points:
        return []

    return format_metering_points(row, metering_points)


def format_metering_points(
    row: pd.Series,
    metering_points: List[Dict[str, Any]],
) -> List[MeteringRecord]:
    """Format retrieved metering points into structured records."""
    return [
        {
            "postalCode": row["postalCode"],
            "streetNumber": row["streetNumber"],
            "streetNumberAddition": meter_point.get("address", {}).get(
                "streetNumberAddition"
            ),
            "bagId": meter_point.get("bagId"),
            "product": meter_point.get("product"),
            "ean": meter_point.get("ean"),
            "specialMeteringPoint": meter_point.get("specialMeteringPoint"),
        }
        for meter_point in metering_points
    ]


def get_metering_points(
    product: str,
    postal_code: str,
    street_number: int,
    street_number_addition: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch metering points from the API."""
    params: Dict[str, Any] = {
        "product": product,
        "postalCode": postal_code,
        "streetNumber": street_number,
    }

    if street_number_addition:
        params["streetNumberAddition"] = street_number_addition

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json().get("meteringPoints", [])

    except requests.RequestException as error:
        st.warning(
            f"API request failed for {postal_code} {street_number} ({product}): {error}"
        )
        return None

    except ValueError as error:
        st.warning(
            f"Could not parse API response for {postal_code} {street_number} "
            f"({product}): {error}"
        )
        return None


def normalize_optional_value(value: Any) -> Optional[str]:
    """Normalize optional CSV values such as street number additions."""
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    return value


def download_csv(updated_df: pd.DataFrame) -> None:
    """Create a downloadable CSV file from the processed data."""
    csv = updated_df.to_csv(index=False)

    st.download_button(
        "Download CSV",
        data=csv,
        file_name="metering_data.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
