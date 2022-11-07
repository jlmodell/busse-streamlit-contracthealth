import streamlit as st
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from pymongo import MongoClient
from io import BytesIO

load_dotenv()

# virtualenv -p="C:\Python310\python.exe" .venv && .\.venv\Scripts\activate && pip install -r requirements.txt

MONGODB_URI = os.getenv("MONGODB_URI")
assert MONGODB_URI is not None, "MONGODB_URI is not set"

client = MongoClient(MONGODB_URI)
db = client.bussepricing

contracts = db.get_collection("contract_prices")
costs = db.get_collection("costs")
customers = db.get_collection("customers")


def filter_pricingagreements(df, item):
    df["pricingagreements"] = df["pricingagreements"].apply(
        lambda x: list(filter(lambda y: y["item"] == item, x))[0].get("price")
    )
    return df


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]

    return df


def get_cost(item):
    cost = costs.find_one({"item": item})
    if not cost:
        raise ValueError("Item not found")
    return cost["cost"]


def get_customer(customerName):
    customer = customers.find_one({"contract_name": customerName})
    if not customer:
        customer = {}

    return (
        customer.get("distributor_fee", 0.05)
        + customer.get("cash_discount_fee", 0.00)
        + customer.get("gpo_fee", 0.00)
    )


@st.cache
def load_data(item: str, contractend: str):
    try:
        contractend = datetime.strptime(contractend, "%Y-%m-%d")
    except Exception:
        raise ValueError("Invalid contract end date, format should be YYYY-MM-DD")

    df = pd.DataFrame(
        list(
            contracts.find(
                {
                    "pricingagreements.item": item,
                    "contractend": {
                        "$gte": contractend,
                    },
                },
                {
                    "pricingagreements": 1,
                    "contractend": 1,
                    "contractname": 1,
                    "contractnumber": 1,
                    "_id": 0,
                    "contractstart": 1,
                },
            )
        )
    )

    df = filter_pricingagreements(df, item)

    # Get the cost
    df["item"] = item
    try:
        df["cost"] = round(df["item"].apply(get_cost), 2)
    except ValueError:
        st.error("Item not found")

    df["customer_fee%"] = round(df["contractname"].apply(get_customer), 2)
    df["safety"] = round(df["cost"] * 0.05, 2)
    df["customer_fee"] = round((df["customer_fee%"] * df["pricingagreements"]), 2)
    df["total_cost"] = round(
        (df["cost"] + df["customer_fee"] + df["safety"]),
        2,
    )

    # Calculate the GP
    df["gp"] = round(df["pricingagreements"] - df["total_cost"], 2)

    # Calculate the GP %
    df["gp%"] = round(df["gp"] / df["pricingagreements"] * 100, 2)

    # Flag for review
    df["review"] = df["gp%"] < 26.9999

    # Sort the data
    df = df.sort_values(by=["contractend", "review"])

    return df


text_item_input = st.text_input("Item")
text_contractend_input = st.text_input("Contract end date YYYY-MM-DD")


def to_excel(df: pd.DataFrame):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="Sheet1", index=False)
    writer.save()
    processed_data = output.getvalue()
    return processed_data


if text_item_input and text_contractend_input:
    data_load_state = st.text("Loading data...")
    df = load_data(item=text_item_input, contractend=text_contractend_input)
    data_load_state.text("Loading data...done!")

    # create excel file for download in bytespace
    df_xlsx = to_excel(df)
    # create filter UI
    df = filter_dataframe(df)

    # Group the data
    chart_data = df.groupby("contractend").count()["contractnumber"]

    st.title("Pricing Agreements Health Check")
    st.markdown(
        """
        This dashboard shows the health of pricing agreements for a given item. It shows the number of contracts that are expiring in a given month and the GP% for each contract. Contracts with a GP% below 27% are flagged for review.
        """
    )

    st.markdown("## Contracts expiring in a given month")
    st.bar_chart(chart_data)

    st.download_button(
        label="📥 Download Current Result",
        data=df_xlsx,
        file_name=f"Item {text_item_input} - Expiring after {text_contractend_input}.xlsx",
    )

    st.markdown("## Pricing Agreements")
    st.dataframe(df)
