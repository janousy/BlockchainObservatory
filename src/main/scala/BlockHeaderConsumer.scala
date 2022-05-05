import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._

import java.io.File


object BlockHeaderConsumer extends App {

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"
  final val KAFKA_HOST: String = "http://172.23.149.211:9092"
  // assuming the VM has mounted a disk on path /mnt
  final val TARGET_OS_PATH: String = "/mnt/delta/bronze/"
  final val KAFKA_TOPIC: String = "algod_indexer_public_block_header_flat"
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

  val algorandBlockHeaderSchema = new StructType()
    .add("ROUND", LongType)
    .add("REALTIME", LongType)
    .add("REWARDSLEVEL", LongType)
    .add("GH", StringType)
    .add("TS", LongType)
    .add("GEN", StringType)
    .add("RND", LongType)
    .add("RWD", StringType)
    .add("TXN", StringType)
    .add("EARN", LongType)
    .add("FRAC", LongType)
    .add("PREV", StringType)
    .add("RATE", LongType)
    .add("SEED", StringType)
    .add("PROTO", StringType)
    .add("RWCALR", LongType)

  val source = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOST)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest") // streaming queries subscribe to latest by default
    .option("failOnDataLoss", false)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  val query = source.select(col("key"), from_json(col("value"), algorandBlockHeaderSchema).alias("header"))
  val data = query.select(col("key"), col("header.*"))

  val writeStream = data.writeStream
    .format("delta")
    .outputMode("append") //default
    .option("checkpointLocation", TARGET_DELTA_TABLE + "/checkpoint")
    .start(TARGET_DELTA_TABLE)

  writeStream.awaitTermination()
}

