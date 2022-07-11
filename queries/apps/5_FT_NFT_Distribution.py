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


    # config for the sparksession
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '12g'),
        ('spark.executor.cores', '2'),
        ('spark.cores.max', '4'),
        ('spark.driver.memory', '2g'),
        ('spark.executor.instances', '1'),
        ('spark.dynamicAllocation.enabled', 'true'),
        ('spark.dynamicAllocation.shuffleTracking.enabled', 'true'),
        ('spark.dynamicAllocation.executorIdleTimeout', '60s'),
        ('spark.dynamicAllocation.minExecutors', '0'),
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
    spark = SparkSession \
        .builder \
        .config(conf=config) \
        .appName("5_countFT_NFT_Application") \
        .master("spark://172.23.149.212:7077") \
        .getOrCreate()

    # create a schema so data quality of dfAsset is ensured
    schema = StructType([
        StructField("df", BooleanType(), True),
        StructField("closed_at", LongType(), True),
        StructField("c", StringType(), True),
        StructField("f", StringType(), True),
        StructField("index", LongType(), True),
        StructField("created_at", LongType(), True),
        StructField("am", StringType(), True),
        StructField("an", StringType(), True),
        StructField("m", StringType(), True),
        StructField("r", StringType(), True),
        StructField("deleted", BooleanType(), True),
        StructField("t", LongType(), True),
        StructField("au", StringType(), True),
        StructField("un", StringType(), True),
        StructField("creator_addr", StringType(), True),
        StructField("dc", LongType(), True),
        ])


    # getting asset table
    dfAsset = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'asset') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .schema(schema) \
        .load()


    # if amount of assets is exactly one than it has to be an NFT
    dfAsset = dfAsset.drop("dc", "df", "_id")

    dfNFT = dfAsset.where(dfAsset.t == 1)

    dfFT = dfAsset.where((dfAsset.t != 1) & (dfAsset.t.isNotNull()))

    dfDeleted = dfAsset.where(dfAsset.deleted == "true")

    # dfUsers = dfAccounts.where((dfAccounts.rewards_total == 0) & (dfAccounts.deleted == False))
    NFTcount = dfNFT.count()
    FTcount = dfFT.count()
    DelCount = dfDeleted.count()


    # newest round for writing to the gold table
    newestRound = dfAsset.agg(F.max("created_at")).collect()[0][0]


    # write amount of Algos in gold table
    # first put value in a df
    result = spark.createDataFrame(
        [
            (NFTcount, FTcount, DelCount, newestRound)  # create your data here, be consistent in the types.

        ],
        ["AmountOfNFT", "AmountOfFT", "AmountOfDeletedAssets", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'AssetsOverview_5') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # all people that have created an NFT on which time
    graph = dfNFT.select("created_at")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    roundsNFT = [row[0] for (row) in graph]


    # min
    minNFTrounds = dfNFT.agg(F.min("created_at")).collect()[0][0]
    maxNFTrounds = dfNFT.agg(F.max("created_at")).collect()[0][0]


    # histogram x-axis round when creating NFT
    # only the NFTs are taken into consideration, which are not already deleted
    # the plot is saved to the VM
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minNFTrounds), np.log10(maxNFTrounds), bin_size)

    plt.figure()
    plt.hist(roundsNFT, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Blockround")
    plt.ylabel("Amount of NFTs")
    plt.title("Distribution of NFT Creation (Blockround)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/distribution_of_NFT_creation_perRound.jpg', dpi=200)
    plt.show()
    plt.close()


    # all people that have created an FT on which time
    graphFTround = dfFT.select("created_at")

    # preparation for graph
    graphFTround = graphFTround.collect()

    # convert row["data"] to only data
    roundsFT = [row[0] for (row) in graphFTround]


    # min
    minFTrounds = dfFT.agg(F.min("created_at")).collect()[0][0]
    maxFTrounds = dfFT.agg(F.max("created_at")).collect()[0][0]


    # histogram x-axis round when creating NFT
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minFTrounds), np.log10(maxFTrounds), bin_size)

    plt.figure()
    plt.hist(roundsFT, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Blockround")
    plt.ylabel("Amount of FTs")
    plt.title("Distribution of FT Creation (Blockround)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/distribution_of_FT_creation_perRound.jpg', dpi=200)
    plt.show()
    plt.close()


    # the expression can also be done in time
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
    dfNFT = dfBlock.join(dfNFT, dfBlock.blockround == dfNFT.created_at, "inner")
    # the same for FT
    dfFT = dfBlock.join(dfFT, dfBlock.blockround == dfFT.created_at, "inner")


    # post the dfNFT and dfFT in the silvertable after joining
    dfNFT.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_silver') \
        .option('spark.mongodb.collection', 'NFT_creationTimeInSec_5') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    dfFT.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_silver') \
        .option('spark.mongodb.collection', 'FT_creationTimeInSec_5') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()


    # all people that have created an NFT on which time
    graph = dfNFT.select("realtime")

    # preparation for graph
    graph = graph.collect()

    # convert row["data"] to only data
    timeNFT = [row[0] for (row) in graph]


    minNFTtime = dfNFT.agg(F.min("realtime")).collect()[0][0]
    maxNFTtime = dfNFT.agg(F.max("realtime")).collect()[0][0]


    # histogram x-axis round when creating NFT
    # how many bars in the histogram should be plotted
    # the graph is save on the vm
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minNFTtime), np.log10(maxNFTtime), bin_size)

    plt.figure()
    plt.hist(timeNFT, bins=mybins)
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Unix Time")
    plt.ylabel("Amount of NFTs")
    plt.title("Distribution of NFT Creation (Unix Time)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/distribution_of_NFT_creation_unixTime.jpg', dpi=200)
    plt.show()
    plt.close()


    # all people that have created an NFT on which time
    graphFTTime = dfFT.select("realtime")

    # preparation for graph
    graphFTTime = graphFTTime.collect()

    # convert row["data"] to only data
    timeFT = [row[0] for (row) in graphFTTime]


    # min
    minFTtime = dfFT.agg(F.min("realtime")).collect()[0][0]
    maxFTtime = dfFT.agg(F.max("realtime")).collect()[0][0]


    # histogram x-axis round when creating FT
    # how many bars in the histogram should be plotted
    # the graph is saved on the vm
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minFTtime), np.log10(maxFTtime), bin_size)

    plt.figure()
    plt.hist(timeFT, bins=mybins)
    # plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Unix Time")
    plt.ylabel("Amount of FTs")
    plt.title("Distribution of FT Creation (Unix Time)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/distribution_of_FT_creation_unixTime.jpg', dpi=200)
    plt.show()
    plt.close()


    # histogram x-axis round when creating NFT
    # only tokens taken into consideration which are still online
    # the graph is saved on the vm
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minFTtime), np.log10(maxFTtime + 1), bin_size)

    plt.figure()
    plt.hist(timeNFT, bins=mybins, alpha=0.5, label="NFT")
    plt.hist(timeFT, bins=mybins, alpha=0.5, label="FT")
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Unix Time")
    plt.ylabel("Amount of Tokens")
    plt.legend(loc="upper right")
    plt.title("NFT vs FT Creation (Unix Time)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/NFT_vs_FT_creation_unixTime.jpg', dpi=200)
    plt.show()
    plt.close()


    plt.figure()
    plt.bar("NFT", NFTcount, width=0.4, color="blue", label="NFT")
    plt.bar("FT", FTcount, width=0.4, color="orange", label="FT")
    plt.title("Amount of NFTs vs FTs", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/amount_NFT_vs_FT.jpg', dpi=200)
    plt.show()
    plt.close()


    # creation (incl. live) vs deleted #unterscheidung zwischen nft und ft nicht möglich weil deletion t == null
    # preparation for histogram when all assets are created and when all assets are deleted
    dfCreated = dfAsset.where(dfAsset.t.isNotNull())

    # preparation for histogram when all assets are created and when all assets are deleted
    graphCreated = dfCreated.select("created_at")

    # preparation for graph
    graphCreated = graphCreated.collect()

    # convert row["data"] to only data
    roundsCreated = [row[0] for (row) in graphCreated]

    # preparation for histogram when all assets are created and when all assets are deleted
    graphDeleted = dfDeleted.select("closed_at")
    graphDeleted = graphDeleted.collect()

    # convert row["data"] to only data
    roundsDeleted = [row[0] for (row) in graphDeleted]

    # min
    minRounds = dfAsset.agg(F.min("created_at")).collect()[0][0]
    maxRounds = dfAsset.agg(F.max("created_at")).collect()[0][0]

    # histogram when all assets are created and when all assets are deleted
    bin_size = 50
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(minRounds), np.log10(maxRounds), bin_size)

    plt.figure()
    plt.hist(roundsCreated, bins=mybins, alpha=0.5, label="created")
    plt.hist(roundsDeleted, bins=mybins, alpha=0.5, label="deleted")
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("Blockround")
    plt.ylabel("Amount of Tokens")
    plt.legend(loc="upper right")
    plt.title("Creation vs. Deletion of Assets (Blockround)", loc='center', pad=None)
    plt.savefig('/home/ubuntu/apps/figures/5_countNFT/Token_creation_vs_deletion.jpg', dpi=200)
    plt.show()
    plt.close()

    spark.stop()
    raise KeyboardInterrupt
