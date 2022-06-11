# setup mongodb and create the required databases with their unique indexes

# mongosh

# initiate replication set for Spark Structured Streaming
# replication must be active in mongod.conf
rs.initiate(
   {
      _id: "rs0",
      version: 1,
      members: [
         { _id: 0, host : "172.23.149.212:27017" }
      ]
   }
)

# create a database
use algorand

# create collections
db.createCollection("account")
db.createCollection("account_asset")
db.createCollection("asset")
db.createCollection("block_header")
db.createCollection("txn")
db.createCollection("app")
db.createCollection("account_app")

# create indices to increase perfomance of mongodb sink ReplaceOneBusinessKeyStrategy write strategy
# NOTE: we don't need text indices, since we can match sting IDs exactly. 
# "text"-indicises will slow down the sinking immensly! 
# NOTE: the sink connectors might have to be paused to created the indices.
db.account.createIndex({addr: 1},{unique: true})
db.account_asset.createIndex({addr: 1, assetid: -1},{unique: true})
db.asset.createIndex({index: -1},{unique: true})
db.block_header.createIndex({round: -1},{unique: true})
db.txn.createIndex({round: -1, intra: -1},{unique: true})
db.app.createIndex({app: -1},{unique: true})
db.account_app.createIndex({app: -1, addr: 1},{unique: true})