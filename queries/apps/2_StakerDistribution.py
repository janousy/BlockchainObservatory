import matplotlib.pyplot as plt
import numpy as np

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import types
from pyspark.sql.types import StructField, StringType, LongType, DoubleType, BooleanType, StructType, IntegerType

if __name__ == '__main__':
    # config for our sparksession
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '8g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '8'),
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
        .appName("2_StakerDistributionApplication") \
        .master("spark://172.23.149.212:7077") \
        .getOrCreate()

    # account table to determine which accounts have received rewards (are miners)
    dfStaker = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'account') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .load()

    # create a schema so data quality of dfTx is ensured
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

    # drop all unneccessary columns
    dfStaker = dfStaker.select("addr", "rewards_total", "created_at")
    # Staker are all accounts which got at least once rewards
    dfStaker = dfStaker.where(dfStaker.rewards_total > 0)

    # calculate hom many algos paid out as rewards

    tRew = dfStaker.agg(F.sum("rewards_total")).collect()[0][0]

    # add column and calculate the proportion of the account to all algos, proportion is in %
    dfStaker = dfStaker.withColumn("proportion", dfStaker.rewards_total / tRew * 100)

    # select necesseray field in dfTx. The fields help to determine when a staker went online and offline
    dfTx = dfTx.select("round", "txn_snd", "txn_type", "txn_votefst")

    # keyreg is either a node which log in to participate in the network or log off
    dfTx = dfTx.filter(dfTx.txn_type == "keyreg")
    # distinguish between online and offline transactions, votefst is null when it was an offline application and otherwise the staker has applied to get online
    dfTx = dfTx.withColumn("status", F.when(F.col('txn_votefst').isNull(), "offline").otherwise("online"))

    # when a staker starts to participate in the network
    # set Rounds to long, so a join later is possible
    dfTx = dfTx.withColumn("participationRound", dfTx["round"].cast("long") + 320)
    # when a staker has applied to participate / Unparticipate in the network
    dfTx = dfTx.withColumn("applicationRound", dfTx["round"].cast("long"))
    # since Round is renamed to applicationRound, the normal Round can be dropped
    dfTx = dfTx.drop("round")

    # write number of stakers in gold table
    # append to get a history over the development
    addresses = dfStaker.count()
    newestRound = dfStaker.agg(F.max("created_at")).collect()[0][0]

    result = spark.createDataFrame(
        [
            (addresses, newestRound)  # create your data here, be consistent in the types.

        ],
        ["NrOfAddresses", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'NumberOfStakers_2') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # write number of stakers in gold table
    # append to get a history over the development
    transactions = dfTx.count()
    newestRound = dfTx.agg(F.max("applicationRound")).collect()[0][0]

    result = spark.createDataFrame(
        [
            (transactions, newestRound)  # create your data here, be consistent in the types.

        ],
        ["NrOfTransactions", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'NumberOfStakerRelatedTransactions_2') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # get BlockHeader to know the Realtime of a Block
    dfBlock = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'block_header') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .load()

    # select necessary schema
    dfBlock = dfBlock.select(col("round").alias("blockround"), col("realtime"))

    # add time to dfTx, where the information about online and offline is stored
    dfTx = dfBlock.join(dfTx, dfBlock.blockround == dfTx.participationRound, "inner")
    # in pyspark an inner join sometimes does not remove the column properly, therefore to be sure one of the columns is dropped
    # additionally txn_type is always keyreg therefore not used anymore, and since we have a status txn_votefst can be removed as well
    dfTx = dfTx.drop("blockround", "txn_type", "txn_votefst")

    # create a dataframe with all online transactions and convert its time to unix time
    dfOnline = dfTx.filter(dfTx.status == "online")
    dfOnline = dfOnline.withColumnRenamed("realtime", "starttime")
    dfOnline = dfOnline.withColumn("starttimeInSec", dfOnline["starttime"])
    # converting in unix time and reordering
    dfOnline = dfOnline.select("txn_snd", "applicationRound", "participationRound",
                               from_unixtime(col("starttime")).alias("starttime"), "starttimeInSec")

    candidacies = dfOnline.count()
    newestRound = dfOnline.agg(F.max("applicationRound")).collect()[0][0]

    result = spark.createDataFrame(
        [
            (candidacies, newestRound)  # create your data here, be consistent in the types.

        ],
        ["TotalCandidates", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'NumberOfStakerCandidates_2') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # graph, histogram x-axis round when starting participating
    graph = dfOnline.select("participationRound")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    rounds = [row[0] for (row) in graph]

    # min
    minParticipationRound = dfOnline.agg(F.min("participationRound")).collect()[0][0]

    maxParticipationRound = dfOnline.agg(F.max("participationRound")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minParticipationRound), np.log10(maxParticipationRound), bin_size)

    plt.figure()
    plt.hist(rounds, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Blockround")
    plt.ylabel("Number of Staker")
    plt.title("Distribution of Staker Starting to Participate (Blockround)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/2_stakerDistribution/Staker_Start_Distribution_Blockround.jpg', dpi=200)
    plt.show()
    plt.close()

    # graph, histogram x-axis unix time when starting
    graph = dfOnline.select("starttimeInSec")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    time = [row[0] for (row) in graph]

    # min
    minUnixTime = dfOnline.agg(F.min("starttimeInSec")).collect()[0][0]

    maxUnixTime = dfOnline.agg(F.max("starttimeInSec")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minUnixTime), np.log10(maxUnixTime), bin_size)

    plt.figure()
    plt.hist(time, bins=mybins)
    # plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Unix Time")
    plt.ylabel("Number of Staker")
    plt.title("Distribution of Staker Starting to Participate (Unix Time)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/2_stakerDistribution/Staker_Start_Distribution_UnixTime.jpg', dpi=200)
    plt.show()
    plt.close()

    # create a dataframe with all online transactions and convert its time to unix time
    dfOffline = dfTx.filter(dfTx.status == "offline")
    dfOffline = dfOffline.withColumnRenamed("realtime", "endtime")
    dfOffline = dfOffline.withColumn("endtimeInSec", dfOffline["endtime"])
    # converting in unix time and reordering
    dfOffline = dfOffline.select("txn_snd", "applicationRound", "participationRound",
                                 from_unixtime(col("endtime")).alias("endtime"), "endtimeInSec")

    # graph, histogram x-axis round when starting participating -- when going offline
    graph = dfOffline.select("participationRound")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    roundsOffline = [row[0] for (row) in graph]

    # min
    minOffParticipationRound = dfOffline.agg(F.min("participationRound")).collect()[0][0]

    maxOffParticipationRound = dfOffline.agg(F.max("participationRound")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minOffParticipationRound), np.log10(maxOffParticipationRound), bin_size)

    plt.figure()
    plt.hist(roundsOffline, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Blockround")
    plt.ylabel("Number of Staker")
    plt.title("Distribution of Staker Ending to Participate (Blockround)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/2_stakerDistribution/Staker_End_Distribution_Blockround.jpg', dpi=200)
    plt.show()
    plt.close()

    # graph, histogram x-axis unix time when starting when going offline
    graph = dfOffline.select("endtimeInSec")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    time = [row[0] for (row) in graph]

    # min
    minOffEndtimeSec = dfOffline.agg(F.min("endtimeInSec")).collect()[0][0]

    maxOffEndtimeSec = dfOffline.agg(F.max("endtimeInSec")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minOffEndtimeSec), np.log10(maxOffEndtimeSec), bin_size)

    plt.figure()
    plt.hist(time, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Unix Time")
    plt.ylabel("Number of Staker")
    plt.title("Distribution of Staker Ending to Participate (Unix Time)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/2_stakerDistribution/Staker_End_Distribution_unixtime.jpg', dpi=200)
    plt.show()
    plt.close()

    silverdf = dfStaker.select("addr", "rewards_total")

    # save staker and their rewards in silver table
    silverdf.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_silver') \
        .option('spark.mongodb.collection', 'distribution_of_stakers_rewards_2') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # graph, histogram rewardsdistribution
    graph = dfStaker.select("rewards_total")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    rewards = [row[0] for (row) in graph]

    # min
    minRewards = dfStaker.agg(F.min("rewards_total")).collect()[0][0]

    maxRewards = dfStaker.agg(F.max("rewards_total")).collect()[0][0]

    # histogram x-axis round when starting participating
    # how many bars in the histogram should be plotted

    bin_size = 50
    # distribute bins log(equally) over the whole data
    # +1 because Rewards can be zero
    mybins = np.logspace(np.log10(minRewards + 1), np.log10(maxRewards), bin_size)

    plt.figure()
    plt.hist(rewards, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Microalgos")
    plt.ylabel("Number of Staker")
    plt.title("Staker Reward Distribution", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/2_stakerDistribution/Staker_reward_distribution.jpg', dpi=200)
    plt.show()
    plt.close()

    spark.stop()
    raise KeyboardInterrupt
