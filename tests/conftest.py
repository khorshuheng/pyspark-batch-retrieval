import pytest
from pyspark.sql import SparkSession


@pytest.yield_fixture(scope="session")
def spark(pytestconfig):
    builder = SparkSession.builder.appName("Batch Retrieval Test")
    spark_master = pytestconfig.getoption("master")
    if spark_master:
        builder = builder.master(spark_master)
    spark_session = builder.getOrCreate()
    yield spark_session
    spark_session.stop()


def pytest_addoption(parser):
    parser.addoption("--master", action="store", default="")
