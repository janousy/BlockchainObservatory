from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, BooleanType

SPARK_MASTER = "spark://172.23.149.212:7077"
KAFKA_HOST = "http://172.23.149.211:9092"

TARGET_OS_PATH = "/mnt/delta/bronze/"
KAFKA_TOPIC = "algod_indexer_public_txn_flat"
TARGET_DELTA_TABLE = TARGET_OS_PATH + KAFKA_TOPIC

val spark = SparkSession \
.builder()
.appName("EXTRACT - " + KAFKA_TOPIC) \
.master(SPARK_MASTER) \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.executor.memory", "3g")
.config("spark.executor.cores", "3")
.config("spark.cores.max", "3")
.getOrCreate()


schema = StructType([
    StructField("ROUND", StringType()),
    StructField("TXID", LongType()),
    StructField("INTRA", LongType()),
    StructField("TYPEENUM", LongType()),
    StructField("ASSET", BooleanType()),
    StructField("EXTRA", IntegerType()),
    StructField("RR", LongType()),
    StructField("SIG", StringType()),
    StructField("TXN_FEE", LongType()),
    StructField("TXN_FV", LongType()),
    StructField("TXN_GH", StringType()),
    StructField("TXN_LV", LongType()),
    StructField("TXN_SND", StringType()),
    StructField("TXN_TYPE", StringType()),
    StructField("TXN_GEN", StringType()),
    StructField("TXN_GRP", StringType()),
    StructField("TXN_LX", StringType()),
    StructField("TXN_NOTE", StringType()),
    StructField("TXN_REKEY", StringType()),
    StructField("TXN_RCV", StringType()),
    StructField("TXN_AMT", StringType()),
    StructField("TXN_CLOSE", StringType()),
    StructField("TXN_VOTEKEY", StringType()),
    StructField("TXN_SELKEY", StringType()),
    StructField("TXN_VOTEFST", LongType()),
    StructField("TXN_VOTELST", LongType()),
    StructField("TXN_VOTEKD", LongType()),
    StructField("TXN_NONPART", BooleanType()),
    StructField("TXN_CAID", LongType()),
    StructField("TXN_APAR", StringType()),
    StructField("TXN_XAID", LongType()),
    StructField("TXN_AAMT", LongType()),
    StructField("TXN_ASND", StringType()),
    StructField("TXN_ARCV", StringType()),
    StructField("TXN_ACLOSE", StringType()),
    StructField("TXN_FADD", StringType()),
    StructField("TXN_FAID", LongType()),
    StructField("TXN_AFRZ", BooleanType()),
    StructField("TXN_APID", LongType()),
    StructField("TXN_APAN", LongType()),
    StructField("TXN_APAT", StringType()),
    StructField("TXN_APAP", StringType()),
    StructField("TXN_APAA", StringType()),
    StructField("TXN_APSU", StringType()),
    StructField("TXN_APFA", StringType()),
    StructField("TXN_APAS", StringType()),
    StructField("TXN_APGS", StringType()),
    StructField("TXN_APLS", StringType()),
    StructField("TXN_APEP", StringType()),
    StructField("TXN_SIG", StringType()),
    StructField("TXN_MSIG", StringType()),
    StructField("TXN_LSIG", StringType())])
