package neo4j

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object GraphBuilder extends App {

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"

  val spark = SparkSession
    .builder()
    .appName("Neo4j Graph-Builder")
    .master(SPARK_MASTER)
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
    .getOrCreate()

  spark.sparkContext.setLogLevel("DEBUG");

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
    .add("txn_fee", LongType)
    .add("txn_fv", LongType)
    .add("txn_gh", StringType)
    .add("txn_lv", LongType)
    .add("txn_snd", StringType)
    .add("txn_type", StringType)
    .add("txn_gen", StringType)
    .add("txn_grp", StringType)
    .add("txn_lx", StringType)
    .add("txn_note", StringType)
    .add("txn_rekey", StringType)
    // Payment Transaction
    .add("txn_rcv", StringType)
    .add("txn_amt", LongType)
    .add("txn_close", StringType)
    // Key Registration Transaction
    .add("txn_votekey", StringType)
    .add("txn_selkey", StringType)
    .add("txn_votefst", LongType)
    .add("txn_votelst", LongType)
    .add("txn_votekd", LongType)
    .add("txn_nonpart", BooleanType)
    // Asset Configuration Transaction
    .add("txn_caid", LongType)
    .add("txn_apar", StringType)
    // Asset Transfer/Clawback/Freeze Transaction
    .add("txn_xaid", LongType)
    .add("txn_aamt", LongType)
    .add("txn_asnd", StringType)
    .add("txn_arcv", StringType)
    .add("txn_aclose", StringType)
    .add("txn_fadd", StringType)
    .add("txn_faid", LongType)
    .add("txn_afrz", BooleanType)
    // Application Call Transaction
    .add("txn_apid", LongType)
    .add("txn_apan", LongType)
    .add("txn_apat", StringType)
    .add("txn_apap", StringType)
    .add("txn_apaa", StringType)
    .add("txn_apsu", StringType)
    .add("txn_apfa", StringType)
    .add("txn_apas", StringType)
    .add("txn_apgs", StringType)
    .add("txn_apls", StringType)
    .add("txn_apep", StringType)
    // Signed Transaction
    .add("txn_sig", StringType)
    .add("txn_msig", StringType)
    .add("txn_lsig", StringType)

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

