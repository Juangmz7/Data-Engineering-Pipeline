import pandera as pa
from pandera import Column, Check

from shared.contracts.validation_schema import DataFrameSchema


class YellowTaxiTripValidationSchema(DataFrameSchema):

    def get_schema(self) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {
                "VendorID": Column(int, Check.isin([1, 2, 6, 7]), coerce=True, required=True),
                "tpep_pickup_datetime": Column(pa.DateTime, coerce=True, required=True),
                "tpep_dropoff_datetime": Column(pa.DateTime, coerce=True, required=True),
                "passenger_count": Column(
                    float,
                    Check.in_range(0, 8),
                    required=True,
                    nullable=True,
                ),
                "trip_distance": Column(float, Check.ge(0), required=True),
                "RatecodeID": Column(
                    float,
                    Check.isin([1, 2, 3, 4, 5, 6, 99]),
                    required=False,
                    nullable=True,
                ),
                "store_and_fwd_flag": Column(
                    str,
                    Check.isin(["Y", "N"]),
                    required=True,
                    nullable=True,
                ),
                "PULocationID": Column(int, Check.ge(1), coerce=True, required=True),
                "DOLocationID": Column(int, Check.ge(1), coerce=True, required=True),
                "payment_type": Column(
                    int,
                    Check.isin([0, 1, 2, 3, 4, 5, 6]),
                    coerce=True,
                    required=True,
                ),
                "fare_amount": Column(
                    float,
                    Check.in_range(-1000, 10000),
                    required=True,
                ),
                "extra": Column(float, Check.ge(0), required=False),
                "mta_tax": Column(float, Check.ge(0), required=True),
                "tip_amount": Column(float, Check.ge(0), required=False),
                "tolls_amount": Column(float, Check.ge(0), required=False),
                "improvement_surcharge": Column(float, Check.ge(0), required=True),
                "total_amount": Column(
                    float,
                    Check.in_range(-1000, 10000),
                    required=True,
                ),
                "congestion_surcharge": Column(
                    float,
                    Check.ge(0),
                    required=False,
                    nullable=True,
                ),
                
                "airport_fee": Column(
                    float,
                    Check.ge(0),
                    required=False,
                    nullable=True,
                ),
                "cbd_congestion_fee": Column(
                    float,
                    Check.ge(0),
                    required=False,
                    nullable=True,
                ),
            },
            checks=[
                Check(
                    lambda df: df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"],
                    error="tpep_dropoff_datetime must be greater than or equal to tpep_pickup_datetime",
                )
            ],
            strict=True,
        )
