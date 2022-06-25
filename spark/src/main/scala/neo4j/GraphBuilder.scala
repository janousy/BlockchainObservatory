package neo4j

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object GraphBuilder extends App {
  val LOGGER = LoggerFactory.getLogger("CustomLogs")

  final val SPARK_MASTER: String = "spark://172.23.149.212:7077"
  final val MONGODB_SOURCE_DB: String = "test"
  final val MONGODB_SOURCE_COLLECT: String = "txn"

  //TODO: change this when converting to stream
  // spark.batch_size should no be too large as MongoDB cursor will time out.
  final val BATCH_SIZE: Int = 5000
  final val MAX_CORES: String = "3"

  val spark = SparkSession
    .builder()
    .appName("Neo4j Graph-Builder MultiThread")
    .master(SPARK_MASTER)
    .config("spark.executor.memory", "32g")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", MAX_CORES)
    .config("spark.driver.memory", "8g")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
    .config("spark.dynamicAllocation.minExecutors", "1")
    .config("spark.dynamicAllocation.maxExecutors", MAX_CORES)
    .config("spark.dynamicAllocation.initialExecutors", "1")
    .config("spark.dynamicAllocation.executorAllocationRatio", "1")
    .config("spark.worker.cleanup.enabled", "true")
    .config("spark.worker.cleanup.interval", "60")
    .config("spark.shuffle.service.db.enabled", "true")
    .config("spark.worker.cleanup.appDataTtl", "60")
    .config("spark.executor.logs.rolling.strategy", "time")
    .config("spark.executor.logs.rolling.time.interval", "hourly")
    .config("spark.executor.logs.rolling.maxRetainedFiles", "3")
    //.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2")
  .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

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

  println("Reading from MongoDB at %s:%s", MONGODB_SOURCE_DB, MONGODB_SOURCE_COLLECT)
  val dfTxn = spark.read.format("mongodb")
    .option("spark.mongodb.connection.uri", "mongodb://172.23.149.212:27017")
    .option("spark.mongodb.database", MONGODB_SOURCE_DB)
    .option("spark.mongodb.collection", MONGODB_SOURCE_COLLECT)
    .option("park.mongodb.read.readPreference.name", "primaryPreferred")
    .option("spark.mongodb.change.stream.publish.full.document.only", "true")
    .option("forceDeleteTempCheckpointLocation", "true")
    .schema(schema)
    .load()

  val dfPaymentTx = dfTxn.filter(col("typeenum") === 1)
    .select(col("txid"), col("txn_snd"), col("txn_rcv"), col("txn_amt"), col("txn_fee"),
      col("round"), col("intra"), col("txn_close"))

  dfPaymentTx.write
    .format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode(SaveMode.Append)
    .option("relationship", "PAYMENT")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Account")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "txn_rcv:account")
    .option("relationship.properties", "txn_amt:amount, txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_close:closedSndAccountTx")
    .option("batch.size", BATCH_SIZE)
    .save()

  var dfKeyRegTx = dfTxn.filter(col("typeenum") === 2)
    .select(col("txid"),
      col("round"),
      col("intra"),
      col("txn_fee"),
      col("txn_snd"),
      col("txn_selkey"),
      col("txn_votefst"),
      col("txn_votekd"),
      col("txn_votekey"),
      col("txn_votelst"))

  dfKeyRegTx = dfKeyRegTx.withColumn("keyRegistrationType",
  when(col("txn_selkey").isNotNull() || col("txn_votefst").isNotNull()
    || col("txn_votekd").isNotNull() || col("txn_votekey").isNotNull()
    || col("txn_votelst").isNotNull(), "online")
    .otherwise("offline"))
  .withColumn("txn_rcv", lit(0))


  dfKeyRegTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "KEY_REGISTRATION")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":ParticipationNode")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, keyRegistrationType:keyRegistrationType")
    .option("batch.size", BATCH_SIZE)
    .save()

  var dfAssetConfigTx = dfTxn.filter(dfTxn.col("typeenum") === 3)
    .select(dfTxn.col("txid"),
      dfTxn.col("round"),
      dfTxn.col("intra"),
      dfTxn.col("txn_fee"),
      dfTxn.col("txn_snd"),
      dfTxn.col("txn_caid"),
      dfTxn.col("txn_apar"),
      dfTxn.col("asset"))

  dfAssetConfigTx = dfAssetConfigTx.withColumn("configurationType",
  when(col("txn_caid").isNull(), "creation")
    .when(col("txn_caid").isNotNull() && col("txn_apar").isNotNull(), "configuration")
    .when(col("txn_caid").isNotNull() && col("txn_apar").isNull(), "destruction")
  )

  dfAssetConfigTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode(SaveMode.Append)
    .option("relationship", "ASSET_CONFIGURATION")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Asset")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:asset")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_caid:assetId, txn_apar:configurationParameters, configurationType:configurationType")
    .option("batch.size", BATCH_SIZE)
    .save()

  var dfAssetTransferTx = dfTxn.filter(dfTxn.col("typeenum") === 4)
    .select(dfTxn.col("txid"),
      dfTxn.col("round"),
      dfTxn.col("intra"),
      dfTxn.col("txn_fee"),
      dfTxn.col("txn_snd"),
      dfTxn.col("txn_arcv"),
      dfTxn.col("txn_aamt"),
      dfTxn.col("txn_asnd"),
      dfTxn.col("asset"),
      dfTxn.col("txn_xaid"))

  dfAssetTransferTx = dfAssetTransferTx.withColumn("transferType",
  when(col("txn_asnd").isNotNull(), "revoke")
    .when(col("txn_snd") === col("txn_arcv"), "opt-in")
    .otherwise("transfer")
  )

  dfAssetTransferTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode(SaveMode.Append)
    .option("relationship", "ASSET_TRANSFER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Account")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "txn_arcv:account")
    .option("relationship.properties", "txn_aamt:amount, txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_xaid:assetId, txn_asnd:assetSenderInRevokingTx, transferType")
    .option("batch.size", BATCH_SIZE)
    .save()

  var dfAssetFreezeTx = dfTxn.filter(col("typeenum") === 5)
    .select(col("txid"),
      col("round"),
      col("intra"),
      col("txn_fee"),
      col("txn_snd"),
      col("txn_afrz"),
      col("txn_fadd"),
      col("txn_faid"),
      col("asset"))

  dfAssetFreezeTx = dfAssetFreezeTx.withColumn("freezeType",
  when(dfAssetFreezeTx.col("txn_afrz") === "true", "freeze")
    .when(dfAssetFreezeTx.col("txn_afrz") === "false", "unfreeze")
  )

  dfAssetFreezeTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode(SaveMode.Append)
    .option("relationship", "ASSET_FREEZE")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Asset")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:asset")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_fadd:frozenAssetAccountHolder, txn_faid:assetIdBeingFrozen, freezeType:freezeType")
    .option("batch.size", BATCH_SIZE)
    .save()

  var dfApplicationCallTx = dfTxn.filter(col("typeenum") === 6)
    .select(col("txid"),
      col("round"),
      col("intra"),
      col("txn_fee"),
      col("txn_snd"),
      col("txn_apid"),
      col("txn_apap"),
      col("txn_apgs"),
      col("txn_apls"),
      col("txn_apsu"),
      col("txn_apan"),
      col("txn_apaa"),
      col("txn_apas"),
      col("txn_apat"),
      col("txn_apfa"),
      col("txn_apep"),
      col("asset"),
      col("txn_note"))


  dfApplicationCallTx = dfApplicationCallTx.withColumn("applicationCallType",
  when(dfApplicationCallTx.col("txn_apan").isNull() && dfApplicationCallTx.col("txn_apid").isNull() && dfApplicationCallTx.col("txn_apap").isNotNull() && dfApplicationCallTx.col("txn_apsu").isNotNull(), "create")
    .when(dfApplicationCallTx.col("txn_apan") === 4, "update")
    .when(dfApplicationCallTx.col("txn_apan") === 5, "delete")
    .when(dfApplicationCallTx.col("txn_apan") === 1, "opt-in")
    .when(dfApplicationCallTx.col("txn_apan") === 2, "close-out")
    .when(dfApplicationCallTx.col("txn_apan") === 3, "clear-state")
    .otherwise("noOp")
  )

  dfApplicationCallTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode(SaveMode.Append)
    .option("relationship", "APPLICATION_CALL")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Application")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:application")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, applicationCallType, txn_apan:applicationCallTypeEnum, txn_apid:applicationId, txn_apap:approvalProgam, txn_apsu:clearProgram, txn_apaa:applicationCallArguments, txn_apat:accountsList, txn_apfa:applicationsList, txn_apas:assetsList")
    .option("batch.size", BATCH_SIZE)
    .save()
}

