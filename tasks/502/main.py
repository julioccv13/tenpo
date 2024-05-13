import json
import argparse

from pyspark.sql.functions import col, ceil, lit, max as s_max
from pyspark.sql.types import StringType, StructType, ArrayType, IntegerType


from core.ports import Process
from core.services.factory_udf import process_json_key_custom_function_udf_factory
from core.spark import Spark
from core.common import fix_key


UNIFORM_PARTITION_COL_NAME = "date_uniform_partition"
COMPRESSION = "snappy"
COLUMNS_TO_PROCESS = ["profile", "eventProps", "deviceInfo"]

# TODO: all_identities line shows null
EXTRACT_SCHEMA = {
    "id": str,
    "eventName": str,
    "ts": int,
    "profile": json.dumps,
    "deviceInfo": json.dumps,
    "eventProps": json.dumps,
}
SCHEMAS = {
    "ts": IntegerType(),
    "profile": StructType()
    .add("all_identities", ArrayType(StringType(), True), True)
    .add("email", StringType(), True)
    .add("identity", StringType(), True)
    .add("name", StringType(), True)
    .add("phone", StringType(), True)
    .add("platform", StringType(), True)
    .add("push_token", StringType(), True),
    "deviceinfo": StructType()
    .add("appversion", StringType(), True)
    .add("browser", StringType(), True)
    .add("dpi", IntegerType(), True)
    .add("make", StringType(), True)
    .add("model", StringType(), True)
    .add("osversion", StringType(), True)
    .add("sdkversion", StringType(), True),
    "eventprops": StructType()
    .add("action", StringType(), True)
    .add("amount", StringType(), True)
    .add("attempt", StringType(), True)
    .add("breakdown", StringType(), True)
    .add("campaign", StringType(), True)
    .add("campaign_id", StringType(), True)
    .add("campaign_name", StringType(), True)
    .add("campaign_type", StringType(), True)
    .add("cantidad_de_comercios", StringType(), True)
    .add("cashoutquantity", StringType(), True)
    .add("categoria", StringType(), True)
    .add("category", StringType(), True)
    .add("code", StringType(), True)
    .add("comercio", StringType(), True)
    .add("compania", StringType(), True)
    .add("comuna", StringType(), True)
    .add("correo", StringType(), True)
    .add("coupon", StringType(), True)
    .add("couponname", StringType(), True)
    .add("ct_app_version", StringType(), True)
    .add("ct_app_version_prev", StringType(), True)
    .add("ct_network_carrier", StringType(), True)
    .add("ct_os_version", StringType(), True)
    .add("ct_sdk_version", StringType(), True)
    .add("ct_session_id", StringType(), True)
    .add("ct_source", StringType(), True)
    .add("description", StringType(), True)
    .add("direccion", StringType(), True)
    .add("divisa", StringType(), True)
    .add("employer_company_name", StringType(), True)
    .add("empresa", StringType(), True)
    .add("fecha", StringType(), True)
    .add("feedback", StringType(), True)
    .add("frecuencia", StringType(), True)
    .add("group", StringType(), True)
    .add("grupo", StringType(), True)
    .add("hasusesuggestion", StringType(), True)
    .add("id", StringType(), True)
    .add("identity", StringType(), True)
    .add("install", StringType(), True)
    .add("intentosrutinvalidos", StringType(), True)
    .add("intentosserieinvalidos", StringType(), True)
    .add("is_cp_related", StringType(), True)
    .add("istotalamount", StringType(), True)
    .add("job", StringType(), True)
    .add("meincluded", StringType(), True)
    .add("message", StringType(), True)
    .add("method", StringType(), True)
    .add("moneda_pesos", StringType(), True)
    .add("monto", StringType(), True)
    .add("monto_faltante_pesos", StringType(), True)
    .add("monto_pesos", StringType(), True)
    .add("nacionalidad", StringType(), True)
    .add("name", StringType(), True)
    .add("nombre", StringType(), True)
    .add("nombre_comercio", StringType(), True)
    .add("numero_de_telefono", StringType(), True)
    .add("ocupacion", StringType(), True)
    .add("posicion", StringType(), True)
    .add("position", StringType(), True)
    .add("product", StringType(), True)
    .add("provider", StringType(), True)
    .add("region", StringType(), True)
    .add("resubscribed", StringType(), True)
    .add("result", StringType(), True)
    .add("rut", StringType(), True)
    .add("saldo", StringType(), True)
    .add("score", StringType(), True)
    .add("screen", StringType(), True)
    .add("sentiment", StringType(), True)
    .add("servicio", StringType(), True)
    .add("session_id", StringType(), True)
    .add("session_length", StringType(), True)
    .add("source", StringType(), True)
    .add("status", StringType(), True)
    .add("subcategoria_n2", StringType(), True)
    .add("subcategoria_n3", StringType(), True)
    .add("subcategoria_n4", StringType(), True)
    .add("subscription_type", StringType(), True)
    .add("suscriptor", StringType(), True)
    .add("tenpocontacts", StringType(), True)
    .add("tenpocontactspercentage", StringType(), True)
    .add("tipo", StringType(), True)
    .add("token", StringType(), True)
    .add("totalcontacts", StringType(), True)
    .add("type", StringType(), True)
    .add("url", StringType(), True)
    .add("user", StringType(), True)
    .add("userid", StringType(), True)
    .add("utility", StringType(), True)
    .add("value", StringType(), True)
    .add("variant", StringType(), True)
    .add("work_occupation", StringType(), True)
    .add("work_status", StringType(), True)
    .add("wzrk_acct_id", StringType(), True)
    .add("wzrk_bc", StringType(), True)
    .add("wzrk_bi", StringType(), True)
    .add("wzrk_c2a", StringType(), True)
    .add("wzrk_cid", StringType(), True)
    .add("wzrk_ck", StringType(), True)
    .add("wzrk_cts", StringType(), True)
    .add("wzrk_dl", StringType(), True)
    .add("wzrk_dt", StringType(), True)
    .add("wzrk_from", StringType(), True)
    .add("wzrk_id", StringType(), True)
    .add("wzrk_pid", StringType(), True)
    .add("wzrk_pivot", StringType(), True)
    .add("wzrk_pn", StringType(), True)
    .add("wzrk_push_amp", StringType(), True)
    .add("wzrk_rnv", StringType(), True)
    .add("wzrk_ttl", StringType(), True)
    .add("wzrk_ttl_s", StringType(), True)
    .add("comment", StringType(), True)
    .add("input_type", StringType(), True)
    .add("label", StringType(), True)
    .add("user_rating", StringType(), True)
    .add("journey_id", StringType(), True)
    .add("journey_name", StringType(), True)
    .add("closemotives", StringType(), True)
    .add("closeaccountreason", StringType(), True),
}


class Process502(Process):
    def __init__(self, **kwargs):
        self.partition_size = kwargs["partition_size"]
        self.source_path = kwargs["process_staging_pattern"]
        self.destination_path_root = kwargs["process_gold_folder"]

    def run(self):
        self._extract_data()
        self._transform_data()
        self._load_data()

    def _extract_data(self):
        rdd = Spark().sparkContext.textFile(self.source_path)
        rdd = rdd.zipWithIndex()
        rdd = rdd.map(lambda x: {"id": x[1], **json.loads(x[0])})
        rdd = rdd.map(
            lambda x: {str(k): EXTRACT_SCHEMA.get(str(k), str)(v) for k, v in x.items()}
        )
        self._dataframe = rdd.toDF()
        self._dataframe.cache()

    def _transform_data(self):
        self._fix_columns_names()
        self._apply_udf_to_columns()
        self._process_partition()

    def _fix_columns_names(self):
        self._dataframe = self._dataframe.select(
            *[col(c).alias(fix_key(c).lower()) for c in self._dataframe.columns]
        )

    def _apply_udf_to_columns(self):
        for column in COLUMNS_TO_PROCESS:
            process_dictionary_udf = process_json_key_custom_function_udf_factory(
                fix_key, schema=SCHEMAS[column.lower()]
            ).function
            self._dataframe = self._dataframe.withColumn(
                column.lower(), process_dictionary_udf(col(column))
            )

    def _process_partition(self):
        self._dataframe = self._dataframe.withColumn(
            UNIFORM_PARTITION_COL_NAME,
            ceil((col("id") + lit(1)) / lit(self.partition_size)).cast(IntegerType()),
        )
        val_max = self._dataframe.agg(s_max(UNIFORM_PARTITION_COL_NAME).alias("s_max"))
        self._repartition_value = val_max.collect()[0]["s_max"]

    def _load_data(self):
        (
            self._dataframe.coalesce(self._repartition_value)
            .write.option("compression", COMPRESSION)
            .format("parquet")
            .mode("overwrite")
            .save(self.destination_path_root)
        )


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-PSP", "--process-staging-pattern", type=str, required=True)
    parser.add_argument("-PGF", "--process-gold-folder", type=str, required=True)
    parser.add_argument("-PS", "--partition-size", type=int, default=500000)
    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    args = get_args()
    process = Process502(**args)

    process.run()
