-- SET 'auto.offset.reset' = 'earliest';
CREATE
OR REPLACE STREAM ALGOD_INDEXER_PUBLIC_TXN_STREAM
(
round BIGINT,
txid VARCHAR,
intra INTEGER,
typeenum INTEGER,
asset BIGINT,
txn VARCHAR,
extra VARCHAR
)
WITH (kafka_topic='algod.indexer.public.txn', value_format='json', partitions=1);

CREATE
OR REPLACE STREAM "algod_indexer_public_txn_flat"
AS
SELECT round,
       txid KEY,
              intra,
              typeenum,
              asset,
              extra,
              CAST(EXTRACTJSONFIELD(txn, '$.rr') AS BIGINT) AS rr,
              CAST(EXTRACTJSONFIELD(txn, '$.sig') AS STRING) AS sig,
-- Common Txn Fields
              CAST(EXTRACTJSONFIELD(txn, '$.txn.fee')  AS BIGINT) AS txn_fee,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.fv')   AS BIGINT) AS txn_fv,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.gh')   AS STRING) AS txn_gh,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.lv')   AS BIGINT) AS txn_lv,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.snd')  AS STRING) AS txn_snd,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.type') AS STRING) AS txn_type,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.gen')  AS STRING) AS txn_gen,
              EXTRACTJSONFIELD(txn, '$.txn.grp') AS txn_grp, --BYTE
              EXTRACTJSONFIELD(txn, '$.txn.lx') AS txn_lx, --BYTE
              EXTRACTJSONFIELD(txn, '$.txn.note') AS txn_note, --BYTE
              CAST(EXTRACTJSONFIELD(txn, '$.txn.rekey') AS STRING) AS txn_rekey,
-- Payment Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.rcv') AS STRING) AS txn_rcv,
              EXTRACTJSONFIELD(txn, '$.txn.amt') AS txn_amt, --BYTE
              CAST(EXTRACTJSONFIELD(txn, '$.txn.close') AS STRING) AS txn_close,
-- Key Registration Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.votekey') AS STRING) AS txn_votekey,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.selkey') AS STRING) AS txn_selkey,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.votefst') AS BIGINT) AS txn_votefst,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.votelst') AS BIGINT) AS txn_votelst,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.votekd') AS BIGINT) AS txn_votekd,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.nonpart') AS BOOLEAN) AS txn_nonpart,
-- Asset Configuration Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.caid') AS BIGINT) AS txn_caid,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apar') AS STRING) AS txn_apar,
-- Asset Transfer/Clawback/Freeze Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.xaid') AS BIGINT) AS txn_xaid,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.aamt') AS BIGINT) AS txn_aamt,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.asnd') AS STRING) AS txn_asnd,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.arcv') AS STRING) AS txn_arcv,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.aclose') AS STRING) AS txn_aclose,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.fadd') AS STRING) AS txn_fadd,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.faid') AS BIGINT) AS txn_faid,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.afrz') AS BOOLEAN) AS txn_afrz,
-- Application Call Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apan') AS BIGINT) AS txn_apan,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apat') AS STRING) AS txn_apat,
              EXTRACTJSONFIELD(txn, '$.txn.apap') AS txn_apap, --BYTE
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apid') AS BIGINT) AS txn_apid,
              EXTRACTJSONFIELD(txn, '$.txn.apaa') AS txn_apaa, --BYTE
              EXTRACTJSONFIELD(txn, '$.txn.apsu') AS txn_apsu, --BYTE
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apfa') AS STRING) AS txn_apfa,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apas') AS STRING) AS txn_apas,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apgs') AS STRING) AS txn_apgs,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apls') AS STRING) AS txn_apls,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.apep') AS STRING) AS txn_apep,
-- Storage_State_Schema
-- EXTRACTJSONFIELD(txn, '$.txn.apep') AS txn_nui,
-- EXTRACTJSONFIELD(txn, '$.txn.nbs') AS txn_nbs,
-- Signed Transaction
              CAST(EXTRACTJSONFIELD(txn, '$.txn.sig') AS STRING) AS txn_sig,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.msig')  AS STRING) AS txn_msig,
              CAST(EXTRACTJSONFIELD(txn, '$.txn.lsig') AS STRING) AS txn_lsig

FROM ALGOD_INDEXER_PUBLIC_TXN_STREAM;
