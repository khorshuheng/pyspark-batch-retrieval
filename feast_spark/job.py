from typing import List, Any, Dict

import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, expr, monotonically_increasing_id


def as_of_join(
    entity: DataFrame,
    join_keys: List[str],
    feature_table: DataFrame,
    features: List[str],
    timestamp_prefix: str = "feature",
    max_age: str = None,
):
    feature_event_timestamp = f"{timestamp_prefix}_event_timestamp"
    entity_with_id = entity.withColumn("_row_nr", monotonically_increasing_id())
    selected_feature_table = feature_table.select(
        join_keys + features + ["event_timestamp", "created_timestamp"]
    ).withColumnRenamed("event_timestamp", feature_event_timestamp)

    join_cond = (
        entity_with_id.event_timestamp
        >= selected_feature_table[feature_event_timestamp]
    )
    if max_age:
        join_cond = join_cond & (
            selected_feature_table[feature_event_timestamp]
            >= entity_with_id.event_timestamp - expr(f"INTERVAL {max_age}")
        )

    for key in join_keys:
        join_cond = join_cond & (entity_with_id[key] == selected_feature_table[key])

    conditional_join = entity_with_id.join(
        selected_feature_table, join_cond, "leftOuter"
    )
    for key in join_keys:
        conditional_join = conditional_join.drop(selected_feature_table[key])

    window = Window.partitionBy("_row_nr", *join_keys).orderBy(
        col(feature_event_timestamp).desc(), col("created_timestamp").desc()
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity.columns + [feature_event_timestamp] + features
    )


def query(
    query_conf: List[Dict[str, Any]], entity: DataFrame, tables: Dict[str, DataFrame]
) -> DataFrame:
    joined = entity
    for query in query_conf:
        joined = as_of_join(
            joined,
            query["join"],
            tables[query["table"]],
            query["features"],
            timestamp_prefix=query["table"],
            max_age=query.get("max_age"),
        )
    return joined


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
    spark = SparkSession.builder.appName("Batch Retrieval").getOrCreate()
    config_file_path = SparkFiles.get("config.json")
    with open(config_file_path, "r") as config_file:
        conf = json.load(config_file)
        batch_retrieval_job(spark, conf)
    spark.stop()
