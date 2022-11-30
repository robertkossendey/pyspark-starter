# pylint: disable=C0114,C0115,C0116,R0903,R0914
from pyspark.sql.functions import col, max as pyspark_max
from pyspark.sql.types import DoubleType

from lib.job import SimpleDeltaLakeJob

SOURCE_LOCATION = "s3a://silver/product"
TARGET_LOCATION = "s3a://gold/product"


class SampleJob(SimpleDeltaLakeJob):

    # pylint: disable=unused-argument
    def process_df(self, data_frame, epoch_id):
        # explicitly select columns that are needed
        product = data_frame.select(
            "product_id",
            "version",
            col("product_price").cast(DoubleType()),
            col("description").alias("product_description"),
        ).cache()

        # get the max version of each product
        max_version = (
            product
            .groupBy("product_id")
            .agg(pyspark_max("version").alias("version"))
        )

        # inner join on the max version to get the latest entry for each product
        latest_product = (
            product
            .join(other=max_version, on=["product_id", "version"], how="inner")
            .drop("version")
        )

        # write the data frame to the table and update the rows
        self.write_stream(
            data_frame=latest_product,
            target_location=TARGET_LOCATION,
            merge_statement="old.product_id = new.product_id"
        )

        # return outcome data frame and target location for testing purposes
        return latest_product, TARGET_LOCATION


if __name__ == "__main__":
    job = SampleJob()
    job.launch(source_location=SOURCE_LOCATION, target_table_name="product")
