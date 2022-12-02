from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import to_date, col, sequence, when, dayofweek, size, date_format, lit, filter as pyspark_filter
from pyspark.sql.types import DoubleType


def calculate_business_hours_difference(
        start_timestamp: str,
        end_timestamp: str,
        new_column_name: str,
        data_frame: DataFrame
) -> DataFrame:
    """This Method calculates the days between 2 timestamps. It excludes weekends."""

    def isWeekday(days: Column) -> Column:
        return lit(1).__lt__(dayofweek(days)) & (lit(7)).__gt__(dayofweek(days))

    weekdays = (
        data_frame
        .withColumn('start_date', to_date(col(start_timestamp)))
        .withColumn('end_date', to_date(col(end_timestamp)))
        .withColumn('days',
                    when(col(start_timestamp) < col(end_timestamp), sequence(col('start_date'), col('end_date')))
                    .otherwise(sequence(col('end_date'), col('start_date'))))
        .withColumn('weekdays', pyspark_filter('days', isWeekday))
        .drop('days')
    )

    business_hours = (
        weekdays.withColumn(new_column_name, (
            when((col(start_timestamp).isNull()) | (col(end_timestamp).isNull()), lit(None))
            .when(col(start_timestamp) < col(end_timestamp),
                  when((dayofweek(col('start_date')).isin([1, 7]))
                       & (dayofweek(col('end_date')).isin([1, 7])),
                       size('weekdays') * 24)
                  .when(dayofweek(col('end_date')).isin([1, 7]),
                        size('weekdays') * 24 - (date_format(col(start_timestamp), 'HH')))
                  .when(dayofweek(col('start_date')).isin([1, 7]),
                        size('weekdays') * 24 - (24 - date_format(end_timestamp, 'HH')))
                  .otherwise(size('weekdays') * 24
                             - (date_format(start_timestamp, ' HH'))
                             - (24 - date_format(end_timestamp, 'HH'))))
            .otherwise(when((dayofweek(col('start_date')).isin([1, 7]))
                            & (dayofweek(col('end_date')).isin([1, 7])),
                            size('weekdays') * -24)
                       .when(dayofweek(col('end_date')).isin([1, 7]),
                             size('weekdays') * -24 + (24 - date_format(col(start_timestamp), 'HH')))
                       .when(dayofweek(col('start_date')).isin([1, 7]),
                             size('weekdays') * -24 + (date_format(end_timestamp, 'HH')))
                       .otherwise(size('weekdays') * -24
                                  + (24 - date_format(start_timestamp, 'HH'))
                                  + (date_format(end_timestamp, ' HH'))))).cast(DoubleType()))
        .drop('start_date', 'end_date', 'weekdays'))

    return business_hours
