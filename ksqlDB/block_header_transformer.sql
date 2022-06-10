-- ksql

-- IMPORTANT: to also include earlier messages:
-- SET 'auto.offset.reset' = 'earliest';

-- using backticks for fields to prevent uppercasing.
CREATE
OR REPLACE STREAM ALGOD_INDEXER_PUBLIC_BLOCK_HEADER_STREAM
(
`round` BIGINT,
`realtime` BIGINT,
`rewardslevel` BIGINT,
`header` STRING
)
WITH (kafka_topic='algod.indexer.public.block_header', value_format='json', partitions=1);

-- create a persistent query to write the stream to a topic
CREATE
OR REPLACE STREAM `algod_indexer_public_block_header_flat`
AS
SELECT `round`,
    `realtime`,
    `rewardslevel`,
    CAST(EXTRACTJSONFIELD(`header`, '$.gh') AS STRING) AS `gh`,
    CAST(EXTRACTJSONFIELD(`header`, '$.ts') AS BIGINT) AS `ts`,
    CAST(EXTRACTJSONFIELD(`header`, '$.gen') AS STRING) AS `gen`,
    CAST(EXTRACTJSONFIELD(`header`, '$.rnd') AS BIGINT) AS `rnd`,
    CAST(EXTRACTJSONFIELD(`header`, '$.rwd') AS STRING) AS `rwd`,
    CAST(EXTRACTJSONFIELD(`header`, '$.txn') AS STRING) AS `txn`,
    CAST(EXTRACTJSONFIELD(`header`, '$.earn') AS BIGINT) AS `earn`,
    CAST(EXTRACTJSONFIELD(`header`, '$.frac') AS BIGINT) AS `frac`,
    CAST(EXTRACTJSONFIELD(`header`, '$.prev') AS STRING) AS `prev`,
    CAST(EXTRACTJSONFIELD(`header`, '$.rate') AS BIGINT) AS `rate`,
    CAST(EXTRACTJSONFIELD(`header`, '$.seed') AS STRING) AS `seed`,
    CAST(EXTRACTJSONFIELD(`header`, '$.proto') AS STRING) AS `proto`,
    CAST(EXTRACTJSONFIELD(`header`, '$.rwcalr') AS BIGINT) AS `rwcalr`
FROM ALGOD_INDEXER_PUBLIC_BLOCK_HEADER_STREAM;


-- DROP STREAM `algod_indexer_public_block_header_flat`;
-- DROP STREAM ALGOD_INDEXER_PUBLIC_BLOCK_HEADER_STREAM;