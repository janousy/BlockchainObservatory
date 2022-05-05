-- SET 'auto.offset.reset' = 'earliest';
CREATE
OR REPLACE STREAM ALGOD_INDEXER_PUBLIC_ASSET_STREAM
(
index BIGINT,
creator_addr VARCHAR,
params VARCHAR,
deleted BOOLEAN,
created_at BIGINT,
closed_at BIGINT
)
WITH (kafka_topic='algod.indexer.public.asset', value_format='json', partitions=1);

CREATE
OR REPLACE STREAM "algod_indexer_public_asset_flat"
AS SELECT
    index,
    creator_addr,
    deleted,
    created_at,
    closed_at,
    CAST(EXTRACTJSONFIELD(params, '$.t') AS BIGINT) AS t,
    CAST(EXTRACTJSONFIELD(params, '$.dc') AS BIGINT) AS dc,
    CAST(EXTRACTJSONFIELD(params, '$.df') AS BOOLEAN) AS df,
    CAST(EXTRACTJSONFIELD(params, '$.un') AS STRING) AS un,
    CAST(EXTRACTJSONFIELD(params, '$.an') AS STRING) AS an,
    CAST(EXTRACTJSONFIELD(params, '$.au') AS STRING) AS au,
    EXTRACTJSONFIELD(params, '$.am') AS am, -- byte array
    CAST(EXTRACTJSONFIELD(params, '$.m') AS STRING) AS m,
    CAST(EXTRACTJSONFIELD(params, '$.r') AS STRING) AS r,
    CAST(EXTRACTJSONFIELD(params, '$.f') AS STRING) AS f,
    CAST(EXTRACTJSONFIELD(params, '$.c') AS STRING) AS c
FROM ALGOD_INDEXER_PUBLIC_ASSET_STREAM;
