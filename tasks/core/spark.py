from pyspark.sql import SparkSession
from pyspark import SparkConf

from .services.generics_udf import udf_list
from .services import Udf


class Spark():
    _instance = None

    def __new__(cls):
        if not cls._instance:
            spark_conf = SparkConf()
            spark_conf.setAppName("SparkETL")
            spark_conf.set("spark.sql.debug.maxToStringFields", 200)
            spark_conf.set("fs.gs.impl",
                           "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            spark_conf.set("fs.AbstractFileSystem.gs.impl",
                           "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            spark_conf.set("spark.driver.memory", "8g")
            spark_conf.set("spark.executor.memory", "4g")
            cls._instance = (
                SparkSession.builder
                .config(conf=spark_conf)
                .appName("SparkETL")
                .getOrCreate())
            for udf in udf_list:
                cls._instance.udf.register(udf.name, udf.function)

        return cls._instance

    def register_new_udf(self, udf: Udf):
        self._instance.udf.register(udf.name, udf.function)
