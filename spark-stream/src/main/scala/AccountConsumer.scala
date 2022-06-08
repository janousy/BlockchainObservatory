import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, month, round, year}
import org.apache.spark.sql.types._

import java.io.File


object AccountConsumer extends App {

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"
  final val KAFKA_HOST: String = "http://172.23.149.211:9092"
  // assuming the VM has mounted a disk on path /mnt
  final val TARGET_OS_PATH: String = "/mnt/delta/bronze/"
  final val KAFKA_TOPIC: String = "algod.indexer.public.account"
  final val TARGET_DELTA_TABLE: String = TARGET_OS_PATH + KAFKA_TOPIC + ".test"

  final val SPARK_PARTITION_SIZE = 10000

  val directory: File = new File(TARGET_OS_PATH);
  if (!(directory.exists())) {
    directory.mkdir();
  }

  val spark = SparkSession
    .builder()
    .appName("KAFKA INGEST - " + TARGET_DELTA_TABLE)
    .master(SPARK_MASTER)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "1")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN");

  val algorandAccountSchema = new StructType()
    .add("addr", StringType)
    .add("microalgos", LongType)
    .add("rewardsbase", LongType)
    .add("rewards_total", LongType)
    .add("deleted", BooleanType)
    .add("created_at", IntegerType)
    .add("closed_at", IntegerType)
    .add("keytype", StringType)
    .add("account_data", StringType)

  val source = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOST)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest") // streaming queries subscribe to latest by default
    .option("failOnDataLoss", value = false)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

  val query = source.select(
    col("key"),
    col("timestamp"),
    from_json(col("value"), algorandAccountSchema).alias("account")
  )

  var data = query.select(
    col("key"),
    col("account.*"),
    col("timestamp").alias("t_kafka"),
  )

  // to be able to partition by create date
  data = data.withColumn("spark_partition",
    round(col("created_at") / SPARK_PARTITION_SIZE).cast(IntegerType))

  val writeStream = data.
    writeStream
    .format("delta")
    .outputMode("append") //default
    // store checkpoints in _ directory to prevent deletion by DELTA VACUUM
    .option("checkpointLocation", TARGET_DELTA_TABLE.replace('_', '.') + "/_checkpoint")
    .partitionBy("spark_partition")
    .start(TARGET_DELTA_TABLE.replace('_', '.'))

  writeStream.awaitTermination()
}
