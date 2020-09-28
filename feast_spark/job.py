from pyspark import SparkFiles
from typing import Dict

from pyspark.sql import SparkSession, DataFrame

import yaml

from feast_spark.transformation.batch_retrieval import query


def batch_retrieval_dataframe(spark: SparkSession, conf: Dict) -> DataFrame:

    entity = conf["entity"]
    entity_df = (
        spark.read.format(entity["format"])
        .options(**entity.get("options", {}))
        .load(entity["path"])
    )

    tables = {
        table_spec["view"]: spark.read.format(table_spec["format"])
        .options(**table_spec.get("options", {}))
        .load(table_spec["path"])
        for table_spec in conf["tables"]
    }

    return query(conf["queries"], entity_df, tables)


def batch_retrieval_job(spark: SparkSession, conf: Dict):
    result = batch_retrieval_dataframe(spark, conf)
    output = conf["output"]
    result.write.format(output["format"]).options(**output.get("options", {})).mode(
        "overwrite"
    ).save(output["path"])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    config_file_path = SparkFiles.get("application.yml")
    with open(config_file_path, "r") as configFile:
        conf = yaml.load(configFile, yaml.Loader)
        batch_retrieval_job(spark, conf)
    spark.stop()
