CREATE OR REPLACE STREAM ALGORAND_TXN_STREAM
(
round BIGINT,
txid VARCHAR,
intra INTEGER,
typeenum INTEGER,
asset BIGINT,
txn VARCHAR,
extra VARCHAR
)
WITH (kafka_topic='algorand-pgsql-delta-noschema-txn', value_format='json', partitions=1);

CREATE OR REPLACE STREAM ALGORAND_TXN_STREAM_FLAT
AS SELECT
              round,
              txid,
              intra,
              typeenum,
              asset,
              extra,
              EXTRACTJSONFIELD(txn, '$.rr') AS rr,
              EXTRACTJSONFIELD(txn, '$.sig') AS sig,
-- Common Txn Fields
              EXTRACTJSONFIELD(txn, '$.txn.fee') AS txn_fee,
              EXTRACTJSONFIELD(txn, '$.txn.fv') AS txn_fv,
              EXTRACTJSONFIELD(txn, '$.txn.gh') AS txn_gh,
              EXTRACTJSONFIELD(txn, '$.txn.lv') AS txn_lv,
              EXTRACTJSONFIELD(txn, '$.txn.snd') AS txn_snd,
              EXTRACTJSONFIELD(txn, '$.txn.type') AS txn_type,
              EXTRACTJSONFIELD(txn, '$.txn.gen') AS txn_gen,
              EXTRACTJSONFIELD(txn, '$.txn.grp') AS txn_grp,
              EXTRACTJSONFIELD(txn, '$.txn.lx') AS txn_lx,
              EXTRACTJSONFIELD(txn, '$.txn.amt') AS txn_amt,
              EXTRACTJSONFIELD(txn, '$.txn.note') AS txn_note,
              EXTRACTJSONFIELD(txn, '$.txn.rekey') AS txn_rekey,
-- Payment Transaction
              EXTRACTJSONFIELD(txn, '$.txn.rcv') AS txn_rcv,
              EXTRACTJSONFIELD(txn, '$.txn.close') AS txn_close,
-- Key Registration Transaction
              EXTRACTJSONFIELD(txn, '$.txn.votekey') AS txn_votekey,
              EXTRACTJSONFIELD(txn, '$.txn.selkey') AS txn_selkey,
              EXTRACTJSONFIELD(txn, '$.txn.votefst') AS txn_votefst,
              EXTRACTJSONFIELD(txn, '$.txn.votelst') AS txn_votelst,
              EXTRACTJSONFIELD(txn, '$.txn.votekd') AS txn_votekd,
              EXTRACTJSONFIELD(txn, '$.txn.nonpart') AS txn_nonpart,
-- Asset Configuration Transaction
              EXTRACTJSONFIELD(txn, '$.txn.caid') AS txn_caid,
              EXTRACTJSONFIELD(txn, '$.txn.apar') AS txn_apar,
-- Asset Transfer/Clawback/Freeze Transaction
              EXTRACTJSONFIELD(txn, '$.txn.xaid') AS txn_xaid,
              EXTRACTJSONFIELD(txn, '$.txn.aamt') AS txn_aamt,
              EXTRACTJSONFIELD(txn, '$.txn.asnd') AS txn_asnd,
              EXTRACTJSONFIELD(txn, '$.txn.arcv') AS txn_arcv,
              EXTRACTJSONFIELD(txn, '$.txn.aclose') AS txn_aclose,
              EXTRACTJSONFIELD(txn, '$.txn.fadd') AS txn_fadd,
              EXTRACTJSONFIELD(txn, '$.txn.faid') AS txn_faid,
              EXTRACTJSONFIELD(txn, '$.txn.afrz') AS txn_afrz,
-- Application Call Transaction
              EXTRACTJSONFIELD(txn, '$.txn.apid') AS txn_apid,
              EXTRACTJSONFIELD(txn, '$.txn.apan') AS txn_apan,
              EXTRACTJSONFIELD(txn, '$.txn.apat') AS txn_apat,
              EXTRACTJSONFIELD(txn, '$.txn.apap') AS txn_apap,
              EXTRACTJSONFIELD(txn, '$.txn.apaa') AS txn_apaa,
              EXTRACTJSONFIELD(txn, '$.txn.apsu') AS txn_apsu,
              EXTRACTJSONFIELD(txn, '$.txn.apfa') AS txn_apfa,
              EXTRACTJSONFIELD(txn, '$.txn.apas') AS txn_apas,
              EXTRACTJSONFIELD(txn, '$.txn.apgs') AS txn_apgs,
              EXTRACTJSONFIELD(txn, '$.txn.apls') AS txn_apls,
              EXTRACTJSONFIELD(txn, '$.txn.apep') AS txn_apep,
-- Storage_State_Schema
-- EXTRACTJSONFIELD(txn, '$.txn.apep') AS txn_nui,
-- EXTRACTJSONFIELD(txn, '$.txn.nbs') AS txn_nbs,
-- Signed Transaction
              EXTRACTJSONFIELD(txn, '$.txn.') AS txn_sig,
              EXTRACTJSONFIELD(txn, '$.txn.') AS txn_msig,
              EXTRACTJSONFIELD(txn, '$.txn.') AS txn_lsig

   FROM ALGORAND_TXN_STREAM;
