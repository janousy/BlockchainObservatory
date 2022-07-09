import matplotlib.pyplot as plt
import numpy as np

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql import types
from pyspark.sql.types import StructField, StringType, LongType, DoubleType, BooleanType, StructType, IntegerType

if __name__ == '__main__':
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '8g'),
        ('spark.executor.cores', '3'),
        ('spark.cores.max', '6'),
        ('spark.driver.memory', '4g'),
        ('spark.executor.instances', '1'),
        ('spark.dynamicAllocation.enabled', 'true'),
        ('spark.dynamicAllocation.shuffleTracking.enabled', 'true'),
        ('spark.dynamicAllocation.executorIdleTimeout', '60s'),
        ('spark.dynamicAllocation.minExecutors', '1'),
        ('spark.dynamicAllocation.maxExecutors', '2'),
        ('spark.dynamicAllocation.initialExecutors', '1'),
        ('spark.dynamicAllocation.executorAllocationRatio', '1'),
        ('spark.worker.cleanup.enabled', 'true'),
        ('spark.worker.cleanup.interval', '60'),
        ('spark.shuffle.service.db.enabled', 'true'),
        ('spark.worker.cleanup.appDataTtl', '60'),
        ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
    ])

    # create sparksession
    # when copying change appName
    spark = SparkSession \
        .builder \
        .config(conf=config) \
        .appName("4_SmartContractDistributionApplication") \
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

    # drop all unnecessary tables
    dfTx = dfTx.select("round", "txn_snd", "txn_type", "txn_apid", "txn_apan", "txn_apas", "txn_apap", "txid")

    # keyreg is either a node which log in to participate in the network or log off
    dfTx = dfTx.filter(dfTx.txn_type == "appl")

    # account table to determine which accounts have received rewards
    dfApp = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'account_app') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .load()

    # txn table to get when an address has applied to participate and when an address have determined the participation
    # dfApp = dfApp.repartition(2)
    # dfApp.rdd.getNumPartitions()
    # drop all unnecessary tables
    dfApp = dfApp.select("app", "deleted", "created_at", "addr")

    # use case based grouping
    # problem two use cases cannot be distinct
    # null --> create
    # null --> noOp transaction
    # 1-> opt in
    # 2->close_out
    # 3-> clear state
    # 4-> update sc
    # 5-> delete sc
    dfTx = dfTx.withColumn("usecase", F.when(F.col('txn_apan') == 1, "opt_in")
                           .when(F.col('txn_apan') == 2, "close_out")
                           .when(F.col('txn_apan') == 3, "clear_state")
                           .when(F.col('txn_apan') == 4, "updateSC")
                           .when(F.col('txn_apan') == 5, "deleteSC")
                           .when((F.col('txn_apan').isNull()) & (F.col('txn_apap').isNotNull()), "createSC")
                           .otherwise("NoOp"))

    applications = dfApp.count()
    newestRound = dfApp.agg(F.max("created_at")).collect()[0][0]

    # write amount of applications in gold table
    result = spark.createDataFrame(
        [
            (applications, newestRound)  # create your data here, be consistent in the types.

        ],
        ["AmountOfApplications", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'ApplicationCount_4') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    dfTxGroupUseCases = dfTx.groupBy("usecase").count()
    # clear_state, close_out, createSC, deleteSC, NoOp, opt_in, updateSC

    # add creation round to every group, so it can be distinguished when the group was saved
    constant = dfApp.agg(F.max("created_at")).collect()[0][0]

    dfTxGroupUseCasesGold = dfTxGroupUseCases.withColumn("CreationRound", F.lit(constant))

    dfTxGroupUseCasesGold.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'ApplicationTransactionsByUseCase_4') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # collect so a python object is created
    graph = dfTxGroupUseCases.collect()

    # graph, histogram x-axis unix time when starting
    # graph = dfTxGroupUseCases.select("count")
    # graph = dfTxGroupUseCases.collect()

    # convert row["data"] to only data
    UCnames = [row[0] for (row) in graph]
    UCvalues = [row[1] for (row) in graph]

    for i in range(len(UCnames)):
        plt.bar(UCnames[i], UCvalues[i], width=0.4)
    plt.title("smart contract use case transcation", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/4_ScDistribution/SC_Use_Cases.jpg', dpi=200)
    plt.show()

    # diagramm wann wie viele smart contract calls gemacht wurden

    graph = dfTx.select("round")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    SCcalls = [row[0] for (row) in graph]

    # calculate the mean of all miner rewards
    mean_round = dfTx.agg(F.mean("round")).collect()[0][0]

    # min
    minSCround = dfTx.agg(F.min("round")).collect()[0][0]

    maxSCround = dfTx.agg(F.max("round")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 100
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minSCround), np.log10(maxSCround), bin_size)
    plt.hist(SCcalls, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("blockround")
    plt.ylabel("number of smart contract calls")
    plt.title("smart contract call distribution", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/4_ScDistribution/SC_Call_Distribution_blockround.jpg', dpi=200)
    plt.show()

    # histogram jeder use case einzeln
    # diagramm wann wie viele smart contract calls gemacht wurden

    graphOptIn = dfTx.filter(dfTx.usecase == "opt_in").select("round")
    graphOptIn = graphOptIn.collect()

    # convert row["data"] to only data
    SCOptIn = [row[0] for (row) in graphOptIn]

    graphCloseOut = dfTx.filter(dfTx.usecase == "close_out").select("round")
    graphCloseOut = graphCloseOut.collect()

    # convert row["data"] to only data
    SCCloseOut = [row[0] for (row) in graphCloseOut]

    graphClear = dfTx.filter(dfTx.usecase == "clear_state").select("round")
    graphClear = graphClear.collect()

    # convert row["data"] to only data
    SCClear = [row[0] for (row) in graphClear]

    graphUpdate = dfTx.filter(dfTx.usecase == "updateSC").select("round")
    graphUpdate = graphUpdate.collect()

    # convert row["data"] to only data
    SCUpdate = [row[0] for (row) in graphUpdate]

    graphDelete = dfTx.filter(dfTx.usecase == "deleteSC").select("round")
    graphDelete = graphDelete.collect()

    # convert row["data"] to only data
    SCDelete = [row[0] for (row) in graphDelete]

    graphCreate = dfTx.filter(dfTx.usecase == "createSC").select("round")
    graphCreate = graphCreate.collect()

    # convert row["data"] to only data
    SCCreate = [row[0] for (row) in graphCreate]

    graphNoOp = dfTx.filter(dfTx.usecase == "NoOp").select("round")
    graphNoOp = graphNoOp.collect()

    # convert row["data"] to only data
    SCNoOp = [row[0] for (row) in graphNoOp]

    # min
    minH = dfTx.agg(F.min("round")).collect()[0][0]
    maxH = dfTx.agg(F.max("round")).collect()[0][0]

    # create the graph
    # histogram x-axis round when creating NFT
    # how many bars in the histogram should be plotted

    bin_size = 100
    # distribute bins log(equally) over the whole data
    # +1 necessary??
    mybins = np.logspace(np.log10(minH), np.log10(maxH) + 1, bin_size)

    plt.hist(SCOptIn, bins=mybins, alpha=0.3, label="opt_in")
    plt.hist(SCCloseOut, bins=mybins, alpha=0.3, label="close_out")
    plt.hist(SCClear, bins=mybins, alpha=0.3, label="clear_state")
    plt.hist(SCUpdate, bins=mybins, alpha=0.3, label="updateSC")
    plt.hist(SCDelete, bins=mybins, alpha=0.3, label="deleteSC")
    plt.hist(SCCreate, bins=mybins, alpha=0.3, label="createSC")
    plt.hist(SCNoOp, bins=mybins, alpha=0.3, label="NoOp")

    # plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True

    plt.xscale('log')
    plt.yscale('log')

    plt.xlabel("blockround")
    plt.ylabel("amount")
    plt.legend(loc="upper right")
    plt.title("usecase distribution", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/4_ScDistribution/UseCaseOverBlockround.jpg', dpi=200)
    plt.show()

    spark.stop()
    raise KeyboardInterrupt
