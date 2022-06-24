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
    //TODO: change this when converting to stream
    .option("batch.size", 5000 * 10)
    .mode(SaveMode.Overwrite)
    .save()

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
    //TODO: change this when converting to stream
    .option("batch.size", 5000 * 10)
    .save()

  dfKeyregTx = dfTxn.filter(col("typeenum") === 2)
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

  from pyspark
  .sql.functions

  import when

  dfKeyregTx = dfKeyregTx.withColumn('keyRegistrationType
  ',
  when(fn.col("txn_selkey").isNotNull() | fn.col("txn_votefst").isNotNull() | fn.col("txn_votekd").isNotNull() | fn.col("txn_votekey").isNotNull() | fn.col("txn_votelst").isNotNull(), "online")
    .otherwise("offline")
  )
  .withColumn('txn_rcv
  ', fn.lit(0)
  )

  dfParticipationNodes = dfKeyregTx.select(dfKeyregTx.txn_rcv.alias("id")).distinct()

  dfParticipationNodes.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":ParticipationNode")
    .option("node.keys", "id")
    .save()

  dfKeyRegAccounts = dfKeyregTx.select(dfKeyregTx.txn_snd.alias("account")).distinct()

  dfKeyRegAccounts.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .save()

  dfKeyregTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "KEY_REGISTRATION")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":ParticipationNode")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, keyRegistrationType:keyRegistrationType")
    .save()

  dfAssetConfigTx = dfTxn.filter(dfTxn.typeenum == 3)
    .select(dfTxn.txid,
      dfTxn.round,
      dfTxn.intra,
      dfTxn.txn_fee,
      dfTxn.txn_snd,
      dfTxn.txn_caid,
      dfTxn.txn_apar,
      dfTxn.asset)

  from pyspark
  .sql.functions

  import when

  dfAssetConfigTx = dfAssetConfigTx.withColumn('configurationType
  ',
  when(fn.col("txn_caid").isNull(), "creation")
    .when(fn.col("txn_caid").isNotNull() & fn.col("txn_apar").isNotNull(), "configuration")
    .when(fn.col("txn_caid").isNotNull() & fn.col("txn_apar").isNull(), "destruction")
  )

  dfAssetAccountsConfig = dfAssetConfigTx.select(dfAssetConfigTx.txn_snd.alias("account")).distinct()

  dfAssetAccountsConfig.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .save()

  dfAssets = dfAssetConfigTx.select(dfAssetConfigTx.asset.alias("asset")).distinct()

  dfAssets.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Asset")
    .option("node.keys", "asset")
    .save()

  dfAssetConfigTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "ASSET_CONFIGURATION")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Asset")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:asset")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_caid:assetId, txn_apar:configurationParameters, configurationType:configurationType")
    .save()

  dfAssetTransferTx = dfTxn.filter(dfTxn.typeenum == 4)
    .select(dfTxn.txid,
      dfTxn.round,
      dfTxn.intra,
      dfTxn.txn_fee,
      dfTxn.txn_snd,
      dfTxn.txn_arcv,
      dfTxn.txn_aamt,
      dfTxn.txn_asnd,
      dfTxn.asset,
      dfTxn.txn_xaid)

  from pyspark
  .sql.functions

  import when

  dfAssetTransferTx = dfAssetTransferTx.withColumn('transferType
  ',
  when(fn.col("txn_asnd").isNotNull(), "revoke")
    .when(fn.col("txn_snd") == fn.col("txn_arcv"), "opt-in")
    .otherwise("transfer")
  )

  dfAssets = dfAssetTransferTx.select(dfAssetTransferTx.txn_xaid.alias("asset")).distinct()

  dfAssets.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Asset")
    .option("node.keys", "asset")
    .save()

  dfTxnSender = dfAssetTransferTx.select(dfAssetTransferTx.txn_snd.alias("account"))
  dfTxnReceiver = dfAssetTransferTx.select(dfAssetTransferTx.txn_arcv.alias("account"))
  dfAssetAccounts = dfTxnSender.union(dfTxnReceiver).distinct()

  dfAssetAccounts.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .save()

  dfAssetTransferTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "ASSET_TRANSFER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Account")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "txn_arcv:account")
    .option("relationship.properties", "txn_aamt:amount, txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_xaid:assetId, txn_asnd:assetSenderInRevokingTx, transferType")
    .save()

  dfAssetFreezeTx = dfTxn.filter(dfTxn.typeenum == 5)
    .select(dfTxn.txid,
      dfTxn.round,
      dfTxn.intra,
      dfTxn.txn_fee,
      dfTxn.txn_snd,
      dfTxn.txn_afrz,
      dfTxn.txn_fadd,
      dfTxn.txn_faid,
      dfTxn.asset)

  from pyspark
  .sql.functions

  import when

  dfAssetFreezeTx = dfAssetFreezeTx.withColumn('freezeType
  ',
  when(fn.col("txn_afrz") == "true", "freeze")
    .when(fn.col("txn_afrz") == "false", "unfreeze")
  )

  dfAssetsFreeze = dfAssetFreezeTx.select(dfAssetFreezeTx.asset.alias("asset")).distinct()

  dfAssetsFreeze.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Asset")
    .option("node.keys", "asset")
    .save()

  dfAssetFreezeAccounts = dfAssetFreezeTx.select(dfAssetFreezeTx.txn_snd.alias("account")).distinct()

  dfAssetFreezeAccounts.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .save()

  dfAssetFreezeTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "ASSET_FREEZE")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Asset")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:asset")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, txn_fadd:frozenAssetAccountHolder, txn_faid:assetIdBeingFrozen, freezeType:freezeType")
    .save()

  dfApplicationCallTx = dfTxn.filter(dfTxn.typeenum == 6)
    .select(dfTxn.txid,
      dfTxn.round,
      dfTxn.intra,
      dfTxn.txn_fee,
      dfTxn.txn_snd,
      dfTxn.txn_apid,
      dfTxn.txn_apap,
      dfTxn.txn_apgs,
      dfTxn.txn_apls,
      dfTxn.txn_apsu,
      dfTxn.txn_apan,
      dfTxn.txn_apaa,
      dfTxn.txn_apas,
      dfTxn.txn_apat,
      dfTxn.txn_apfa,
      dfTxn.txn_apep,
      dfTxn.asset,
      dfTxn.txn_note)

  from pyspark
  .sql.functions

  import when

  dfApplicationCallTx = dfApplicationCallTx.withColumn('applicationCallType
  ',
  when(fn.col("txn_apan").isNull() & fn.col("txn_apid").isNull() & fn.col("txn_apap").isNotNull() & fn.col("txn_apsu").isNotNull(), "create")
    .when(fn.col("txn_apan") == 4, "update")
    .when(fn.col("txn_apan") == 5, "delete")
    .when(fn.col("txn_apan") == 1, "opt-in")
    .when(fn.col("txn_apan") == 2, "close-out")
    .when(fn.col("txn_apan") == 3, "clear-state")
    .otherwise("noOp")
  )

  dfApplications = dfApplicationCallTx.select(dfApplicationCallTx.asset.alias("application")).distinct()

  dfApplications.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Application")
    .option("node.keys", "application")
    .save()

  dfApplicationAccounts = dfApplicationCallTx.select(dfApplicationCallTx.txn_snd.alias("account")).distinct()

  dfApplicationAccounts.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://172.23.149.212:7687")
    .option("labels", ":Account")
    .option("node.keys", "account")
    .save()

  dfApplicationCallTx.write.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://172.23.149.212:7687")
    .mode("Append")
    .option("relationship", "APPLICATION_CALL")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Account")
    .option("relationship.source.save.mode", "Overwrite")
    .option("relationship.source.node.keys", "txn_snd:account")
    .option("relationship.target.labels", ":Application")
    .option("relationship.target.save.mode", "Overwrite")
    .option("relationship.target.node.keys", "asset:application")
    .option("relationship.properties", "txn_fee:fee, round:blockNumber, intra:intraBlockTxNumber, txid:txId, applicationCallType, txn_apan:applicationCallTypeEnum, txn_apid:applicationId, txn_apap:approvalProgam, txn_apsu:clearProgram, txn_apaa:applicationCallArguments, txn_apat:accountsList, txn_apfa:applicationsList, txn_apas:assetsList")
    .save()
}

