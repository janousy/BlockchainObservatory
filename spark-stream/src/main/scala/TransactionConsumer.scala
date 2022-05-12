import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}

import java.io.File


object TransactionConsumer extends App {

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"
  final val KAFKA_HOST: String = "http://172.23.149.211:9092"
  // assuming the VM has mounted a disk on path /mnt
  final val TARGET_OS_PATH: String = "/mnt/delta/bronze/"
  final val KAFKA_TOPIC: String = "algod_indexer_public_txn_flat"
  final val TARGET_DELTA_TABLE: String = TARGET_OS_PATH + KAFKA_TOPIC

  val directory: File = new File(TARGET_OS_PATH);
  if (!(directory.exists())) {
    directory.mkdir();
  }

  val spark = SparkSession
    .builder()
    .appName("EXTRACT - " + KAFKA_TOPIC)
    .master(SPARK_MASTER)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "1")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN");

  val algorandTransactionSchema = new StructType()
    .add("ROUND", StringType)
    .add("TXID", LongType)
    .add("INTRA", LongType)
    .add("TYPEENUM", LongType)
    .add("ASSET", BooleanType)
    .add("EXTRA", StringType)
    .add("RR", LongType)
    .add("SIG", StringType)
    // Common Txn Fields
    .add("TXN_FEE", LongType)
    .add("TXN_FV", LongType)
    .add("TXN_GH", StringType)
    .add("TXN_LV", LongType)
    .add("TXN_SND", StringType)
    .add("TXN_TYPE", StringType)
    .add("TXN_GEN", StringType)
    .add("TXN_GRP", StringType)
    .add("TXN_LX", StringType)
    .add("TXN_NOTE", StringType)
    .add("TXN_REKEY", StringType)
    // Payment Transaction
    .add("TXN_RCV", StringType)
    .add("TXN_AMT", StringType)
    .add("TXN_CLOSE", StringType)
    // Key Registration Transaction
    .add("TXN_VOTEKEY", StringType)
    .add("TXN_SELKEY", StringType)
    .add("TXN_VOTEFST", LongType)
    .add("TXN_VOTELST", LongType)
    .add("TXN_VOTEKD", LongType)
    .add("TXN_NONPART", BooleanType)
    // Asset Configuration Transaction
    .add("TXN_CAID", LongType)
    .add("TXN_APAR", StringType)
    // Asset Transfer/Clawback/Freeze Transaction
    .add("TXN_XAID", LongType)
    .add("TXN_AAMT", LongType)
    .add("TXN_ASND", StringType)
    .add("TXN_ARCV", StringType)
    .add("TXN_ACLOSE", StringType)
    .add("TXN_FADD", StringType)
    .add("TXN_FAID", LongType)
    .add("TXN_AFRZ", BooleanType)
    // Application Call Transaction
    .add("TXN_APID", LongType)
    .add("TXN_APAN", LongType)
    .add("TXN_APAT", StringType)
    .add("TXN_APAP", StringType)
    .add("TXN_APAA", StringType)
    .add("TXN_APSU", StringType)
    .add("TXN_APFA", StringType)
    .add("TXN_APAS", StringType)
    .add("TXN_APGS", StringType)
    .add("TXN_APLS", StringType)
    .add("TXN_APEP", StringType)
    // Signed Transaction
    .add("TXN_SIG", StringType)
    .add("TXN_MSIG", StringType)
    .add("TXN_LSIG", StringType)

  val source = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOST)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest") // streaming queries subscribe to latest by default
    .option("failOnDataLoss", false)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  val query = source.select(col("key"), from_json(col("value"), algorandTransactionSchema).alias("txn"))
  val data = query.select(col("key"), col("txn.*"))

  val writeStream = data.writeStream
    .format("delta")
    .outputMode("append") // default
    .option("checkpointLocation", TARGET_DELTA_TABLE + "/checkpoint")
    .start(TARGET_DELTA_TABLE)

  writeStream.awaitTermination()
}
