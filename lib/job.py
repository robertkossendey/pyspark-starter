# pylint: skip-file
from abc import abstractmethod, ABC

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


class Job(ABC):
    def __init__(self, spark=None):
        self.spark = spark or SparkSession.builder.getOrCreate()


class SimpleDeltaLakeJob(Job, ABC):
    def __init__(self, spark=None):
        super().__init__(spark)

    @abstractmethod
    def process_df(self, data_frame, epoch_id) -> (DataFrame, str):
        pass

    def load_static_data_frame(self, table_location: str) -> DataFrame:
        return (
            self.spark
            .read
            .format("delta")
            .load(table_location)
        )

    def write_stream(self, data_frame: DataFrame, target_location: str, merge_statement: str):
        if not DeltaTable.isDeltaTable(self.spark, target_location):
            (
                data_frame
                .coalesce(1)
                .write
                .format("delta")
                .mode("overwrite")
                .save(target_location)
            )

            delta_table = DeltaTable.forPath(self.spark, target_location)

            delta_table.generate("symlink_format_manifest")
        else:
            delta_table = DeltaTable.forPath(self.spark, target_location)

            (
                delta_table
                .alias("old")
                .merge(data_frame.alias("new"), merge_statement)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            delta_table.generate("symlink_format_manifest")

    def launch(self, source_location: str, target_table_name: str):
        (
            self.spark
            .readStream
            .format("delta")
            .load(source_location)
            .writeStream
            .trigger(once=True)
            .format("delta")
            .option("checkpointLocation", f"{source_location}/_checkpoint/{target_table_name}")
            .option("streamName", target_table_name)
            .foreachBatch(self.process_df)
            .start()
        )
