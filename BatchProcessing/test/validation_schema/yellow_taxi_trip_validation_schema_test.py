import pytest
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

from BatchProcessing.src.validation_schema.yellow_taxi_trip_validation_schema import YellowTaxiTripValidationSchema


@pytest.fixture
def schema_provider():
    """Provides a fresh instance of the validation schema."""
    return YellowTaxiTripValidationSchema()


@pytest.fixture
def valid_dataframe():
    """
    Provides a strictly valid DataFrame meeting all schema requirements.
    This serves as the baseline for all mutation tests.
    """
    df = pd.read_parquet("BatchProcessing/data/yellow_tripdata_2025-01.parquet")
    df = df.rename(columns={"Airport_fee": "airport_fee"})

    valid_df = df[
        df["VendorID"].isin([1, 2, 6, 7])
        & (df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"])
        & (df["passenger_count"].isna() | df["passenger_count"].between(0, 8))
        & (df["trip_distance"] >= 0)
        & (df["RatecodeID"].isna() | df["RatecodeID"].isin([1, 2, 3, 4, 5, 6, 99]))
        & (df["store_and_fwd_flag"].isna() | df["store_and_fwd_flag"].isin(["Y", "N"]))
        & (df["PULocationID"] >= 1)
        & (df["DOLocationID"] >= 1)
        & df["payment_type"].isin([0, 1, 2, 3, 4, 5, 6])
        & df["fare_amount"].between(-1000, 10000)
        & (df["extra"].isna() | (df["extra"] >= 0))
        & (df["mta_tax"] >= 0)
        & (df["tip_amount"].isna() | (df["tip_amount"] >= 0))
        & (df["tolls_amount"].isna() | (df["tolls_amount"] >= 0))
        & (df["improvement_surcharge"] >= 0)
        & df["total_amount"].between(-1000, 10000)
        & (df["congestion_surcharge"].isna() | (df["congestion_surcharge"] >= 0))
        & (df["airport_fee"].isna() | (df["airport_fee"] >= 0))
        & (df["cbd_congestion_fee"].isna() | (df["cbd_congestion_fee"] >= 0))
    ]

    return valid_df.head(1000).reset_index(drop=True)


class TestYellowTaxiTripValidationSchema:

    def test_get_schema_returns_pandera_dataframe_schema(self, schema_provider):
        """Verifies that the factory method returns the correct Pandera object."""
        schema = schema_provider.get_schema()
        assert isinstance(schema, pa.DataFrameSchema)

    def test_valid_dataframe_passes_validation(self, schema_provider, valid_dataframe):
        """Tests the happy path where a fully compliant DataFrame passes."""
        schema = schema_provider.get_schema()
        # Pandera returns the validated DataFrame on success
        validated_df = schema.validate(valid_dataframe)

        assert isinstance(validated_df, pd.DataFrame)
        assert len(validated_df) == len(valid_dataframe)

    def test_strict_mode_rejects_extra_columns(self, schema_provider, valid_dataframe):
        """Ensures strict=True blocks DataFrames with undeclared columns."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df["City"] = ["New York"] * len(df)

        with pytest.raises(SchemaError, match="column 'City' not in"):
            schema.validate(df)

    def test_rejects_missing_mandatory_columns(self, schema_provider, valid_dataframe):
        """Ensures required=True is enforced for mandatory columns."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.drop(columns=["VendorID"])

        with pytest.raises(SchemaError, match="column 'VendorID' not in"):
            schema.validate(df)

    def test_allows_missing_optional_columns(self, schema_provider, valid_dataframe):
        """Verifies that optional columns can be safely omitted."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.drop(columns=["RatecodeID", "airport_fee"], errors="ignore")

        validated_df = schema.validate(df)
        assert "RatecodeID" not in validated_df.columns

    def test_rejects_invalid_categorical_values(self, schema_provider, valid_dataframe):
        """Verifies Check.isin() enforces allowed categorical values."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df.loc[df.index[0], "payment_type"] = 999

        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_rejects_out_of_range_numerics(self, schema_provider, valid_dataframe):
        """Verifies Check.in_range() protects against business logic violations and overflows."""
        schema = schema_provider.get_schema()

        # Test lower bound violation
        df_low = valid_dataframe.copy()
        df_low.loc[df_low.index[0], "trip_distance"] = -1
        with pytest.raises(SchemaError):
            schema.validate(df_low)

        # Test upper bound violation
        df_high = valid_dataframe.copy()
        df_high.loc[df_high.index[0], "fare_amount"] = 20000
        with pytest.raises(SchemaError):
            schema.validate(df_high)

    def test_rejects_invalid_datetime_order(self, schema_provider, valid_dataframe):
        """Verifies the dataframe-level check enforces pickup before or equal to dropoff."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df.loc[df.index[0], "tpep_dropoff_datetime"] = df.loc[df.index[0], "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)

        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_datetime_columns_coerce_to_datetime(self, schema_provider, valid_dataframe):
        """Verifies that coerce=True successfully converts datetime-like values to DateTime objects."""
        schema = schema_provider.get_schema()
        df = valid_dataframe.copy()
        df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].astype(str)
        df["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"].astype(str)

        validated_df = schema.validate(df)

        assert pd.api.types.is_datetime64_any_dtype(validated_df["tpep_pickup_datetime"])
        assert pd.api.types.is_datetime64_any_dtype(validated_df["tpep_dropoff_datetime"])
