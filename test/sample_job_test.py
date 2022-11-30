from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.sample_job import SampleJob


def test_data_frame_processing(spark, mocker):
    mocker.patch('src.sample_job.SampleJob.write_stream', return_value=None)
    job = SampleJob()

    source_data = [
        (
            "1",
            "This is an expensive product",
            1,
            "200.00",
            "FOOD"
        ),
        (
            "1",
            "This is a very expensive product",
            2,
            "400.00",
            "FOOD"
        )
    ]

    source_schema = StructType([
        StructField("product_id", StringType()),
        StructField("description", StringType()),
        StructField("version", IntegerType()),
        StructField("product_price", StringType()),
        StructField("product_category", StringType()),
    ])

    source_df = spark.createDataFrame(
        data=source_data,
        schema=source_schema
    )

    expected_data = [
        (
            "1",
            "This is a very expensive product",
            400.00
        )
    ]

    expected_schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_description", StringType()),
        StructField("product_price", DoubleType()),
    ])

    expected_df = spark.createDataFrame(
        data=expected_data,
        schema=expected_schema
    )

    actual_df, target_location = job.process_df(source_df, '0')

    assert target_location == 's3a://gold/product'

    assert_df_equality(actual_df, expected_df, ignore_column_order=True)
