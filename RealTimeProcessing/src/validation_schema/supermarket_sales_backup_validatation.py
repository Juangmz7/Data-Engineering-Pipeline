import numpy as np
import pandas as pd
import pandera as pa
from pandera import Column, Check

from shared.contracts.validation_schema import DataFrameSchema

class SupermarketSalesBackupValidationSchema(DataFrameSchema):

    def get_schema(self) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {
                "Invoice ID": Column(str, Check.str_length(min_value=1), required=True),
                "Branch": Column(str, Check.str_length(min_value=1), required=True),
                "City": Column(str, Check.str_length(min_value=1), required=True),
                "Customer type": Column(str, Check.isin(["Member", "Normal"]), required=True),
                "Product line": Column(str, Check.str_length(min_value=1), required=True),
                "Unit price": Column(float, Check.in_range(0.0, 100000.0), required=True),
                "Quantity": Column(int, Check.in_range(1, 10000), coerce=True, required=True),
                "Tax 5%": Column(float, Check.ge(0.0), required=True),
                "Sales": Column(float, Check.ge(0.0), required=True),
                "Date": Column(pa.DateTime, coerce=True, required=True),
                "Time": Column(pa.DateTime, coerce=True, required=True),
                "Payment": Column(str, Check.isin(["Cash", "Credit card", "Ewallet"]), required=True),
                "cogs": Column(float, Check.ge(0.0), required=True),
                "gross margin percentage": Column(float, Check.ge(0.0), required=True),
                "gross income": Column(float, Check.ge(0.0), required=True),
                "Total Sale": Column(float, Check.ge(0.0), required=True),
                "Time of the day": Column(
                    str,
                    Check.isin(["Night", "Morning", "Afternoon", "Evening"]),
                    required=True,
                ),
                "Day of the week": Column(
                    str,
                    Check.isin(
                        [
                            "Monday",
                            "Tuesday",
                            "Wednesday",
                            "Thursday",
                            "Friday",
                            "Saturday",
                            "Sunday",
                        ]
                    ),
                    required=True,
                ),
            },
            checks=[
                Check(
                    lambda df: (df["Total Sale"] - (df["Unit price"] * df["Quantity"])).abs() <= 1e-6,
                    error="Total Sale must match Unit price * Quantity",
                ),
                Check(
                    lambda df: df["Time of the day"] == pd_cut_time_of_day(df["Time"].dt.hour),
                    error="Time of the day must match the processed time-of-day categorization",
                ),
                Check(
                    lambda df: df["Day of the week"] == df["Date"].dt.day_name(),
                    error="Day of the week must match Date.dt.day_name()",
                ),
            ],
            strict=True,
        )


def pd_cut_time_of_day(hours):
    categorized = pd.cut(
        hours,
        bins=[-1, 5, 11, 17, 23],
        labels=["Night", "Morning", "Afternoon", "Evening"],
        ordered=False,
    )
    return categorized.astype(str)
