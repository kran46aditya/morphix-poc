from typing import Any, Dict, Optional
import json

from pyspark.sql import SparkSession


def _build_spark(mongo_uri: str) -> SparkSession:
    # Create SparkSession configured for MongoDB Spark Connector. This assumes
    # the Mongo Spark connector jar is available on the classpath. In many
    # deploys you'll add packages like 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
    spark = (
        SparkSession.builder
        .appName("mongo-reader")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        .config("spark.mongodb.input.uri", mongo_uri)
        .config("spark.mongodb.output.uri", mongo_uri)
        .getOrCreate()
    )
    return spark


def read_with_spark(mongo_uri: str, database: str, collection: str, query: Optional[Dict[str, Any]] = None, limit: int = 10) -> Dict[str, Any]:
    """Read from MongoDB into a Spark DataFrame and return a small summary.

    Returns a dict with e.g. schema and preview row count. This function tries to
    keep Spark lifecycle short: it will stop the session it creates.
    """
    uri = mongo_uri
    spark = _build_spark(uri)
    try:
        reader = (
            spark.read.format("mongo")
            .option("uri", uri)
            .option("database", database)
            .option("collection", collection)
        )
        if query:
            reader = reader.option("pipeline", json.dumps([{"$match": query}]))

        df = reader.load()
        preview = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
        schema = df.schema.jsonValue()
        count = df.count()
        return {"schema": schema, "preview_count": len(preview), "preview": preview, "total_count": count}
    finally:
        try:
            spark.stop()
        except Exception:
            pass
