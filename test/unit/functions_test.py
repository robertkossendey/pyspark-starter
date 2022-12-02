import datetime
from chispa import assert_df_equality
from pyspark.sql.types import StructField, TimestampType, StructType, DoubleType

from lib.functions import calculate_business_hours_difference


def test_calculate_business_hours_difference(spark):
    thursday24_12 = datetime.datetime.strptime('2021-06-24T12:00:00', '%Y-%m-%dT%H:%M:%S')
    friday25_11 = datetime.datetime.strptime('2021-06-25T11:00:00', '%Y-%m-%dT%H:%M:%S')
    saturday26_11 = datetime.datetime.strptime('2021-06-26T11:00:00', '%Y-%m-%dT%H:%M:%S')
    sunday27_11 = datetime.datetime.strptime('2021-06-27T11:00:00', '%Y-%m-%dT%H:%M:%S')
    sunday27_next_week = datetime.datetime.strptime('2021-07-04T11:00:00', '%Y-%m-%dT%H:%M:%S')
    monday28_10 = datetime.datetime.strptime('2021-06-28T10:00:00', '%Y-%m-%dT%H:%M:%S')
    tuesday29_13_15 = datetime.datetime.strptime('2021-06-29T13:15:00', '%Y-%m-%dT%H:%M:%S')
    monday_9_next_week = datetime.datetime.strptime('2021-07-05T9:00:00', '%Y-%m-%dT%H:%M:%S')
    friday_23 = datetime.datetime.strptime('2021-07-23T20:00:00', '%Y-%m-%dT%H:%M:%S')
    monday_26 = datetime.datetime.strptime('2021-07-26T20:00:00', '%Y-%m-%dT%H:%M:%S')
    saturday24 = datetime.datetime.strptime('2021-07-24T06:51:00', '%Y-%m-%dT%H:%M:%S')
    tuesday27 = datetime.datetime.strptime('2021-07-27T00:00:00', '%Y-%m-%dT%H:%M:%S')
    tuesday27_8 = datetime.datetime.strptime('2021-07-27T08:00:00', '%Y-%m-%dT%H:%M:%S')

    source_data = [
        (saturday26_11, None),
        (thursday24_12, friday25_11),
        (friday25_11, monday28_10),
        (thursday24_12, monday28_10),
        (thursday24_12, tuesday29_13_15),
        (saturday26_11, sunday27_11),
        (saturday26_11, monday28_10),
        (thursday24_12, saturday26_11),
        (thursday24_12, sunday27_11),
        (sunday27_11, sunday27_next_week),
        (monday28_10, monday_9_next_week),
        (thursday24_12, monday_9_next_week),
        (friday_23, monday_26),
        (saturday24, tuesday27),
        (tuesday27_8, tuesday27),
        (friday25_11, thursday24_12),
        (monday28_10, friday25_11),
        (monday28_10, thursday24_12),
        (tuesday29_13_15, thursday24_12),
        (sunday27_11, saturday26_11),
        (monday28_10, saturday26_11),
        (saturday26_11, thursday24_12),
        (sunday27_11, thursday24_12),
        (sunday27_next_week, sunday27_11,),
        (monday_9_next_week, monday28_10),
        (monday_9_next_week, thursday24_12),
        (monday_26, friday_23),
        (tuesday27, saturday24),
    ]

    schema = StructType([
        StructField("start_timestamp", TimestampType()),
        StructField("end_timestamp", TimestampType()),
    ])

    source_df = spark.createDataFrame(
        data=source_data,
        schema=schema
    )

    expected_data = [
        (saturday26_11, None, None),
        (thursday24_12, friday25_11, 23.0),
        (friday25_11, monday28_10, 23.0),
        (thursday24_12, monday28_10, 46.0),
        (thursday24_12, tuesday29_13_15, 73.0),
        (saturday26_11, sunday27_11, 0.0),
        (saturday26_11, monday28_10, 10.0),
        (thursday24_12, saturday26_11, 36.0),
        (thursday24_12, sunday27_11, 36.0),
        (sunday27_11, sunday27_next_week, 120.0),
        (monday28_10, monday_9_next_week, 119.0),
        (thursday24_12, monday_9_next_week, 165.0),
        (friday_23, monday_26, 24.0),
        (saturday24, tuesday27, 24.0),
        (tuesday27_8, tuesday27, -8.0),
        (friday25_11, thursday24_12, -23.0),
        (monday28_10, friday25_11, -23.0),
        (monday28_10, thursday24_12, -46.0),
        (tuesday29_13_15, thursday24_12, -73.0),
        (sunday27_11, saturday26_11, 0.0),
        (monday28_10, saturday26_11, -10.0),
        (saturday26_11, thursday24_12, -36.0),
        (sunday27_11, thursday24_12, -36.0),
        (sunday27_next_week, sunday27_11, -120.0),
        (monday_9_next_week, monday28_10, -119.0),
        (monday_9_next_week, thursday24_12, -165.0),
        (monday_26, friday_23, -24.0),
        (tuesday27, saturday24, -24.0),
    ]

    expected_schema = StructType([
        StructField("start_timestamp", TimestampType()),
        StructField("end_timestamp", TimestampType()),
        StructField("diff", DoubleType()),
    ])

    expected_df = spark.createDataFrame(
        data=expected_data,
        schema=expected_schema
    )

    actual_df = calculate_business_hours_difference(
        start_timestamp="start_timestamp",
        end_timestamp="end_timestamp",
        new_column_name="diff",
        data_frame=source_df
    )

    assert_df_equality(actual_df, expected_df)
