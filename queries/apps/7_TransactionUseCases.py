import matplotlib.pyplot as plt
import numpy as np

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import types
from pyspark.sql.types import StructField, StringType, LongType, DoubleType, BooleanType, StructType, IntegerType

from pyspark.sql.functions import col, hex, base64, avg, collect_list, concat, lit, max

if __name__ == '__main__':
    # config for our sparksession
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '16g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '8'),
        ('spark.driver.memory', '2g'),
        ('spark.executor.instances', '1'),
        ('spark.dynamicAllocation.enabled', 'true'),
        ('spark.dynamicAllocation.shuffleTracking.enabled', 'true'),
        ('spark.dynamicAllocation.executorIdleTimeout', '60s'),
        ('spark.dynamicAllocation.minExecutors', '2'),
        ('spark.dynamicAllocation.maxExecutors', '2'),
        ('spark.dynamicAllocation.initialExecutors', '1'),
        ('spark.dynamicAllocation.executorAllocationRatio', '1'),
        ('spark.worker.cleanup.enabled', 'true'),
        ('spark.worker.cleanup.interval', '60'),
        ('spark.shuffle.service.db.enabled', 'true'),
        ('spark.worker.cleanup.appDataTtl', '60rite'),
        ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
    ])

    # create sparksession
    # when copying change appName
    spark = SparkSession \
        .builder \
        .config(conf=config) \
        .appName("7_TransactionUseCases") \
        .master("spark://172.23.149.212:7077") \
        .getOrCreate()

    # create a schema so all data quality is ensured
    schema = StructType([StructField("_id", StringType(), True), StructField("asset", LongType(), True),
                         StructField("extra", StringType(), True), StructField("intra", LongType(), True),
                         StructField("round", LongType(), True), StructField("rr", LongType(), True),
                         StructField("sig", StringType(), True), StructField("txid", StringType(), True),
                         StructField("txn_aamt", LongType(), True), StructField("txn_aclose", StringType(), True),
                         StructField("txn_afrz", BooleanType(), True), StructField("txn_amt", LongType(), True),
                         StructField("txn_apaa", StringType(), True), StructField("txn_apan", LongType(), True),
                         StructField("txn_apap", StringType(), True), StructField("txn_apar", StringType(), True),
                         StructField("txn_apas", StringType(), True), StructField("txn_apat", StringType(), True),
                         StructField("txn_apep", StringType(), True), StructField("txn_apfa", StringType(), True),
                         StructField("txn_apgs", StringType(), True), StructField("txn_apid", LongType(), True),
                         StructField("txn_apls", StringType(), True), StructField("txn_apsu", StringType(), True),
                         StructField("txn_arcv", StringType(), True), StructField("txn_asnd", StringType(), True),
                         StructField("txn_caid", LongType(), True), StructField("txn_close", StringType(), True),
                         StructField("txn_fadd", StringType(), True), StructField("txn_faid", LongType(), True),
                         StructField("txn_fee", LongType(), True), StructField("txn_fv", LongType(), True),
                         StructField("txn_gen", StringType(), True), StructField("txn_gh", StringType(), True),
                         StructField("txn_grp", StringType(), True), StructField("txn_lsig", StringType(), True),
                         StructField("txn_lv", LongType(), True), StructField("txn_lx", StringType(), True),
                         StructField("txn_msig", StringType(), True), StructField("txn_nonpart", BooleanType(), True),
                         StructField("txn_note", StringType(), True), StructField("txn_rcv", StringType(), True),
                         StructField("txn_rekey", StringType(), True), StructField("txn_selkey", StringType(), True),
                         StructField("txn_sig", StringType(), True), StructField("txn_snd", StringType(), True),
                         StructField("txn_type", StringType(), True), StructField("txn_votefst", LongType(), True),
                         StructField("txn_votekd", LongType(), True), StructField("txn_votekey", StringType(), True),
                         StructField("txn_votelst", LongType(), True), StructField("txn_xaid", LongType(), True),
                         StructField("typeenum", IntegerType(), True)])

    # account table to determine which accounts have received rewards
    dfTx = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'txn') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .schema(schema) \
        .load()

    # use case based grouping
    # 1-> payment transaction
    # 2-> Keyreg transaction
    # 3-> asset configuration
    # 4-> asset transfer
    # 5-> asset freeze
    # 6-> application transactions
    dfTx = dfTx.withColumn("usecase", F.when(F.col('typeenum') == 1, "payment")
                           .when(F.col('typeenum') == 2, "keyreg")
                           .when(F.col('typeenum') == 3, "assetconfig")
                           .when(F.col('typeenum') == 4, "assettransfer")
                           .when(F.col('typeenum') == 5, "assetfreeze")
                           .when(F.col('typeenum') == 6, "application"))

    dfTxGroupUseCases = dfTx.groupBy("usecase").count()

    # newest round has to be calculated
    newestRound = dfTx.agg(F.max("created_at")).collect()[0][0]
    dfTxGroupUseCasesGold = dfTxGroupUseCases.withColumn("CreationRound", F.lit(newestRound))

    dfTxGroupUseCasesGold.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'TransactionsByUseCase_7') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # collect so a python object is created
    graph = dfTxGroupUseCases.collect()

    # convert row["data"] to only data
    UCnames = [row[0] for (row) in graph]
    UCvalues = [row[1] for (row) in graph]

    plt.figure()
    for i in range(len(UCnames)):
        plt.bar(UCnames[i], UCvalues[i], width=0.4)

    plt.title("Smart Contract Use Case Transactions", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/7_TransactionUC/TC_Use_Cases.jpg', dpi=200)
    plt.show()
    plt.close()

    spark.stop()
    raise KeyboardInterrupt
