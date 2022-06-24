package neo4j

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object GraphBuilder extends App {

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"

  val spark = SparkSession
    .builder()
    .appName("Neo4j Graph Builder")
    .master(SPARK_MASTER)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.executor.memory", "36g")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "1")
    .config("spark.driver.memory", "5g")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
    .config("spark.dynamicAllocation.minExecutors", "0")
    .config("spark.dynamicAllocation.maxExecutors", "1")
    .config("spark.dynamicAllocation.initialExecutors", "1")
    .config("spark.dynamicAllocation.executorAllocationRatio", "1")
    .config("spark.worker.cleanup.enabled", "true")
    .config("spark.worker.cleanup.interval", "60")
    .config("spark.shuffle.service.db.enabled", "true")
    .config("spark.worker.cleanup.appDataTtl", "60")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2")
    .getOrCreate()

  val schema = new StructType()
    .add("round", LongType)
    .add("txid", StringType)
    .add("intra", LongType)
    .add("typeenum", LongType)
    .add("asset", LongType)
    .add("extra", StringType)
    .add("rr", LongType)
    .add("sig", StringType)
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
    .add("TXN_AMT", LongType)
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

  val dfTxn = spark.read.format("mongodb")
    .option("spark.mongodb.connection.uri", "mongodb://172.23.149.212:27017")
    .option("spark.mongodb.database", "algorand")
    .option("spark.mongodb.collection", "txn")
    .option("park.mongodb.read.readPreference.name", "primaryPreferred")
    .option("spark.mongodb.change.stream.publish.full.document.only", "true")
    .option("forceDeleteTempCheckpointLocation", "true")
    .schema(schema)
    .load()

  val dfPaymentTx = dfTxn.filter(col("typeenum") === 1)
    .select(col("txid"), col("txn_snd"), col("txn_rcv"), col("txn_amt"), col("txn_fee"),
      col("round"), col("intra"), col("txn_close"))

  val dfTxnSender = dfPaymentTx.select(col("txn_snd").alias("account"))
  val dfTxnReceiver = dfPaymentTx.select(col("txn_rcv").alias("account"))
  val dfPaymentAccounts = dfTxnSender.union(dfTxnReceiver).distinct()

  dfPaymentAccounts.write.
    format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .mode(SaveMode.Overwrite)
    .save()
}

