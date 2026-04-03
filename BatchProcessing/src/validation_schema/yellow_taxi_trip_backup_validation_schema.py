import numpy as np
import pandas as pd
import pandera as pa
from pandera import Check, Column

from shared.contracts.validation_schema import DataFrameSchema


class YellowTaxiTripBackupValidationSchema(DataFrameSchema):

    def get_schema(self) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {
                "tpep_pickup_datetime": Column(pa.DateTime, coerce=True, required=True),
                "tpep_dropoff_datetime": Column(pa.DateTime, coerce=True, required=True),
                "passenger_count": Column(
                    float,
                    Check.in_range(0, 8),
                    required=True,
                    nullable=True,
                ),
                "trip_distance": Column(float, Check.ge(0), required=True),
                "PULocationID": Column(int, Check.ge(1), coerce=True, required=True),
                "DOLocationID": Column(int, Check.ge(1), coerce=True, required=True),
                "payment_type": Column(int, Check.isin([0, 1, 2, 3, 4, 5, 6]), coerce=True, required=True),
                "fare_amount": Column(float, Check.in_range(-1000, 10000), required=True),
                "extra": Column(float, Check.ge(0), required=False),
                "mta_tax": Column(float, Check.ge(0), required=True),
                "tip_amount": Column(float, Check.ge(0), required=False),
                "tolls_amount": Column(float, Check.ge(0), required=False),
                "improvement_surcharge": Column(float, Check.ge(0), required=True),
                "total_amount": Column(float, Check.in_range(-1000, 10000), required=True),
                "congestion_surcharge": Column(float, Check.ge(0), required=False),
                "airport_fee": Column(float, Check.ge(0), required=False),
                "cbd_congestion_fee": Column(float, Check.ge(0), required=False),
                "trip_duration_minutes": Column(float, Check.ge(0), required=True),
                "pickup_year": Column(int, Check.in_range(2000, 2100), coerce=True, required=True),
                "pickup_month": Column(int, Check.in_range(1, 12), coerce=True, required=True),
                "trip_time_of_day": Column(
                    str,
                    Check.isin(["Night", "Morning", "Afternoon", "Evening"]),
                    required=True,
                ),
                "average_speed_mph": Column(float, Check.ge(0), required=True),
                "revenue_per_mile": Column(float, Check.ge(0), required=True),
                "trip_distance_category": Column(
                    str,
                    Check.isin(["Short", "Medium", "Long"]),
                    required=True,
                ),
                "fare_category": Column(
                    str,
                    Check.isin(["Low", "Medium", "High"]),
                    required=True,
                ),
            },
            checks=[
                Check(
                    lambda df: df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"],
                    error="tpep_dropoff_datetime must be greater than or equal to tpep_pickup_datetime",
                ),
                Check(
                    lambda df: np.isclose(
                        df["trip_duration_minutes"],
                        (
                            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
                        ).dt.total_seconds() / 60.0,
                        rtol=1e-6,
                        atol=1e-6,
                    ),
                    error="trip_duration_minutes must match the pickup/dropoff datetime difference in minutes",
                ),
                Check(
                    lambda df: df["pickup_year"] == df["tpep_pickup_datetime"].dt.year,
                    error="pickup_year must match tpep_pickup_datetime.dt.year",
                ),
                Check(
                    lambda df: df["pickup_month"] == df["tpep_pickup_datetime"].dt.month,
                    error="pickup_month must match tpep_pickup_datetime.dt.month",
                ),
                Check(
                    lambda df: df["trip_time_of_day"]
                    == pd_cut_time_of_day(df["tpep_pickup_datetime"].dt.hour),
                    error="trip_time_of_day must match the pickup hour categorization",
                ),
                Check(
                    lambda df: np.isclose(
                        df["average_speed_mph"],
                        np.where(
                            df["trip_duration_minutes"] > 0,
                            df["trip_distance"] / (df["trip_duration_minutes"] / 60.0),
                            0.0,
                        ),
                        rtol=1e-6,
                        atol=1e-6,
                    ),
                    error="average_speed_mph must match the processed formula",
                ),
                Check(
                    lambda df: np.isclose(
                        df["revenue_per_mile"],
                        np.where(
                            df["trip_distance"] > 0,
                            df["total_amount"] / df["trip_distance"],
                            0.0,
                        ),
                        rtol=1e-6,
                        atol=1e-6,
                    ),
                    error="revenue_per_mile must match the processed formula",
                ),
                Check(
                    lambda df: df["trip_distance_category"]
                    == np.select(
                        [
                            df["trip_distance"] < 2.0,
                            (df["trip_distance"] >= 2.0) & (df["trip_distance"] <= 10.0),
                            df["trip_distance"] > 10.0,
                        ],
                        ["Short", "Medium", "Long"],
                        default="Unknown",
                    ),
                    error="trip_distance_category must match the configured trip distance buckets",
                ),
                Check(
                    lambda df: df["fare_category"]
                    == np.select(
                        [
                            df["fare_amount"] < 20.0,
                            (df["fare_amount"] >= 20.0) & (df["fare_amount"] <= 50.0),
                            df["fare_amount"] > 50.0,
                        ],
                        ["Low", "Medium", "High"],
                        default="Unknown",
                    ),
                    error="fare_category must match the configured fare amount buckets",
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
