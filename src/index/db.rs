use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bitcoin::{OutPoint, Transaction, Txid};
use mysql::{from_row, params, Params, Pool, PooledConn, Statement};
use mysql::prelude::{BinQuery, Queryable, WithParams};
use once_cell::sync::OnceCell;

use crate::{InscriptionId, Sat, SatPoint};
use crate::index::entry::{Entry, InscriptionEntry, OutPointValue, SatRange};

pub struct MBlock {
  pub hash: String,
  pub height: u64,
}

pub struct MOutpoint {
  pub id: u64,
  pub txid: String,
  pub vout: String,
  pub value: Option<u64>,
}

pub struct MSat {
  pub id: u64,
  pub n: u64,
}

pub struct MSatPoint {
  pub id: u64,
  pub outpoint_id: u64,
  pub offset: u64,
}

pub struct MInscription {
  pub id: u64,
  pub inscription_id: String,
  pub offset: u64,
  pub number: u64,
  pub fee: u64,
  pub height: u64,
  pub sat_id: Option<u64>,
  pub sat_point_id: Option<u64>,
  pub timestamp: u64,
}

static DB_POOL: OnceCell<Pool> = OnceCell::new();

#[derive(Debug, Clone, Copy)]
pub struct DB {}

impl DB {
  pub fn new(mysql_url: String) -> Self {
    /// create mysql connection pool
    DB_POOL.set(Pool::new(mysql_url.clone().as_str()).expect(&format!("Error Connection to {}", mysql_url))).unwrap_or_else(|_| { log::info!("try insert pool cell failure!") });
    Self {}
  }

  /// 开启 mysql 模式
  fn get_connection(&self) -> PooledConn {
    DB_POOL.get().expect("Error get pool from OneCell<Pool>").get_conn().expect("Error get_connect from db pool")
  }

  pub fn get_current_height(&self) -> u64 {
    let mut conn = self.get_connection();
    let query_result = conn.query_first("select hash, height from blocks order by height desc").map(|row| {
      row.map(|(hash, height)| MBlock {
        hash,
        height,
      })
    }).unwrap();

    if let Some(block) = query_result {
      block.height + 1
    } else {
      0
    }
  }

  pub fn get_mblock_by_height(&self, height: u64) -> Option<MBlock> {
    let mut conn = self.get_connection();
    conn.exec_first("select hash, height from blocks where height = :height order by height desc", params! {"height" => height}).map(|row| {
      row.map(|(hash, height)| MBlock {
        hash,
        height,
      })
    }).unwrap()
  }

  pub fn insert_mblock(&self, height: &u64, hash: String) {
    let mut conn = self.get_connection();
    conn.exec_drop("insert ignore into blocks (hash, height) values (:hash, :height)", params! {"height" => height, "hash" => hash}).unwrap();
  }

  pub fn insert_outpoint(&self, txid: String, vout: u32) -> u64 {
    let mut conn = self.get_connection();
    conn.exec_drop("insert ignore into outpoints (txid, vout) values (:txid, :vout)", params! {"txid" => txid, "vout" => vout}).unwrap();
    conn.last_insert_id()
  }

  pub fn get_outpoint(&self, txid: String, vout: u32) -> Option<MOutpoint> {
    let mut conn = self.get_connection();

    conn.exec_first("select id, txid, vout, value from outpoints where txid = :txid and vout = :vout", params! {"txid" => txid.clone(), "vout" => vout}).map(|row| {
      row.map(|(id, txid, vout, value)| MOutpoint {
        id,
        txid,
        vout,
        value,
      })
    }).unwrap()
  }

  pub fn get_outpoint_value(&self, txid: String, vout: u32) -> Option<u64> {
    match self.get_outpoint(txid, vout) {
      None => {
        None
      }
      Some(outpoint) => {
        outpoint.value
      }
    }
  }

  pub fn get_outpoint_sat_ranges(&self, outpointvalue: &OutPointValue) -> Vec<u8> {
    log::trace!("get_outpoint_to_sat_ranges");

    let mut conn = self.get_connection();
    let outpoint: OutPoint = Entry::load(*outpointvalue);
    let outpoint_txid = outpoint.txid.to_string();
    let outpoint_vout = outpoint.vout;

    let ret = match conn.exec_first::<String, &str, Params>("select sat_ranges from outpoints where txid = :txid and vout = :vout", params! {
      "txid" => outpoint_txid,
      "vout" => outpoint_vout
    }).unwrap() {
      None => {
        vec![]
      }
      Some(sat_ranges) => {
        hex::decode(sat_ranges).unwrap()
      }
    };

    ret
  }

  pub fn update_outpoint_sat_ranges(&self, outpointvalue: &OutPointValue, sats: &Vec<u8>) {
    log::trace!("update_outpoint_sat_ranges");

    let mut conn = self.get_connection();
    let outpoint: OutPoint = Entry::load(*outpointvalue);
    let outpoint_txid = outpoint.txid.to_string();
    let outpoint_vout = outpoint.vout;

    conn.exec_drop("insert into outpoints (txid, vout, sat_ranges) values (:txid, :vout, :sat_ranges) on duplicate key update sat_ranges = :sat_ranges",
                   params! {"txid" => outpoint_txid, "vout" => outpoint_vout, "sat_ranges" => hex::encode(sats)}).unwrap();
  }

  pub fn batch_insert_outpoint_to_sat_ranges(&self, range_cache: &HashMap<OutPointValue, Vec<u8>>) {
    log::trace!("batch_insert_outpoint_to_sat_ranges");

    let mut conn = self.get_connection();

    conn.exec_batch(
      "insert into outpoints (txid, vout, sat_ranges) values (:txid, :vout, :sat_ranges) on duplicate key update sat_ranges = :sat_ranges",
      range_cache.iter().map(|(outpointvalue, sats)| {
        let outpoint: OutPoint = Entry::load(*outpointvalue);
        let outpoint_txid = outpoint.txid.to_string();
        let outpoint_vout = outpoint.vout;

        params! {
          "txid" => outpoint_txid.clone(), "vout" => outpoint_vout, "sat_ranges" => hex::encode(sats)
        }
      }),
    ).unwrap();
  }

  pub fn insert_outpoint_to_value(&self, outpoint: &OutPoint, value: &u64) {
    let mut conn = self.get_connection();
    let txid = outpoint.txid.to_string();
    conn.exec_drop("insert into outpoints (txid, vout, value) values (:txid, :vout, :value) on duplicate key update value = :value", params! {"txid" => txid.clone(), "value" => value}).unwrap();
  }

  pub fn batch_insert_outpoint_to_value(&self, value_cache: &HashMap<OutPoint, u64>) {
    let mut conn = self.get_connection();

    conn.exec_batch("insert into outpoints (txid, vout, value) values (:txid, :vout, :value) on duplicate key update value = :value",
                    value_cache.iter().map(|(outpoint, value)| {
                      params! {"txid" => outpoint.txid.to_string(), "vout" => outpoint.vout, "value" => value}
                    })).unwrap();
  }

  pub fn insert_sat(&self, n: u64) -> u64 {
    log::trace!("insert_sat");
    let mut conn = self.get_connection();
    conn.exec_drop("insert ignore into sats (n) values (:n)", params! {"n" => n}).unwrap();
    conn.last_insert_id()
  }

  pub fn get_sat(&self, n: u64) -> Option<MSat> {
    let mut conn = self.get_connection();

    conn.exec_first("select id, n from sats where n = :n", params! {"n" => n}).map(|row| {
      row.map(|(id, n)| MSat { id, n })
    }).unwrap()
  }

  pub fn insert_sat_point(&self, sat_point: &SatPoint) -> u64 {
    log::trace!("insert_sat_point");
    let outpoint_txid = sat_point.outpoint.txid.to_string();
    let outpoint_vout = sat_point.outpoint.vout;
    let outpoint_id = match self.get_outpoint(outpoint_txid.clone(), outpoint_vout) {
      None => {
        self.insert_outpoint(outpoint_txid, outpoint_vout)
      }
      Some(outpoint) => {
        outpoint.id
      }
    };

    let mut conn = self.get_connection();
    conn.exec_drop("insert ignore into sat_points (outpoint_id, offset) values (:outpoint_id, :offset)", params! {"outpoint_id" => outpoint_id, "offset" => sat_point.offset}).unwrap();
    conn.last_insert_id()
  }

  pub fn get_sat_point(&self, sat_point: &SatPoint) -> Option<MSatPoint> {
    let outpoint_txid = sat_point.outpoint.txid.to_string();
    let outpoint_vout = sat_point.outpoint.vout;
    let outpoint_id = match self.get_outpoint(outpoint_txid.clone(), outpoint_vout) {
      None => {
        self.insert_outpoint(outpoint_txid, outpoint_vout)
      }
      Some(outpoint) => {
        outpoint.id
      }
    };

    let mut conn = self.get_connection();

    conn.exec_first("select id, outpoint_id, offset from sat_points where outpoint_id = :outpoint_id", params! {"outpoint_id" => outpoint_id}).map(|row| {
      row.map(|(id, outpoint_id, offset)| MSatPoint { id, outpoint_id, offset })
    }).unwrap()
  }


  pub fn insert_sat_to_satpoint(&self, start: &u64, sat_point: &SatPoint) -> u64 {
    log::trace!("insert_sat_to_satpoint");
    let mut conn = self.get_connection();

    let sat_id = match self.get_sat(start.clone()) {
      None => {
        self.insert_sat(start.clone())
      }
      Some(s) => {
        s.id
      }
    };

    let sat_point_id = match self.get_sat_point(sat_point) {
      None => {
        self.insert_sat_point(sat_point)
      }
      Some(s_p) => {
        s_p.id
      }
    };

    conn.exec_drop("insert ignore into sat_to_sat_point (sat_id, sat_point_id) values (:sat_id, :sat_point_id)", params! {
      "sat_id" => sat_id, "sat_point_id" => sat_point_id}).unwrap();
    conn.last_insert_id()
  }

  pub fn insert_transaction(&self, txid: String, content: &Vec<u8>) {
    log::trace!("insert_transaction");
    let mut conn = self.get_connection();
    conn.exec_drop("insert into transactions (txid, content) values (:txid, :content) on duplicate key update content = :content", params! {"txid" => txid, "content" => hex::encode(content)}).unwrap();
  }

  pub fn get_transaction(&self, txid: String) -> Option<Transaction> {
    let mut conn = self.get_connection();
    match conn.exec_first::<String, &str, Params>("select content from transactions where txid = :txid", params! {
      "txid" => txid,
    }).unwrap() {
      None => {
        None
      }
      Some(sat_ranges) => {
        let h = hex::decode(sat_ranges).unwrap();
        let transaction = bitcoin::consensus::deserialize(&h).map_err(|e| {
          anyhow!("Result for batched JSON-RPC response not valid bitcoin tx: {e}")
        });

        Some(transaction.unwrap())
      }
    }
  }

  pub fn get_inscription(&self, inscription_id: &InscriptionId) -> Option<MInscription> {
    let mut conn = self.get_connection();
    conn.exec_first("select id, inscription_id, offset, number, fee, height, sat_id, sat_point_id, timestamp from inscriptions where inscription_id = :inscription_id and index = :index", params! {
      "inscription_id" => inscription_id.txid.to_string(),
      "index" => inscription_id.index
    }).map(|row| {
      row.map(|(id, inscription_id, offset, number, fee, height, sat_id, sat_point_id, timestamp)| MInscription {
        id,
        inscription_id,
        offset,
        number,
        fee,
        height,
        sat_id,
        sat_point_id,
        timestamp,
      })
    }).unwrap()
  }

  pub fn insert_inscription(&self, inscription_id: &InscriptionId, inscription_entry: &InscriptionEntry, sat_point: &SatPoint) -> u64 {
    log::trace!("insert_inscription");
    let sat_id = if let Some(sat) = inscription_entry.sat {
      match self.get_sat(sat.0) {
        None => {
          self.insert_sat(sat.0)
        }
        Some(sat) => {
          sat.id
        }
      }
    } else {
      0
    };

    let sat_point_id = match self.get_sat_point(sat_point) {
      None => {
        self.insert_sat_point(sat_point)
      }
      Some(s_p) => {
        s_p.id
      }
    };

    let mut conn = self.get_connection();

    conn.exec_drop("insert ignore into inscriptions (inscription_id, offset, number, fee, height, timestamp, sat_id, sat_point_id) values (:inscription_id, :offset, :number, :fee, :height, :timestamp, :sat_id, :sat_point_id)", params! {
      "inscription_id" => inscription_id.txid.to_string(),
      "offset" => inscription_id.index,
      "number" => inscription_entry.number,
      "fee" => inscription_entry.fee,
      "height" => inscription_entry.height,
      "timestamp" => inscription_entry.timestamp,
      "sat_id" => sat_id,
      "sat_point_id" => sat_point_id
    }).unwrap();
    conn.last_insert_id()
  }

  pub fn truncate(&self, table_name: String) {
    log::trace!("truncate table {}", table_name.clone());
    let mut conn = self.get_connection();
    // "truncate ?".with((table_name, )).run(conn).unwrap();
    conn.exec_drop("truncate blocks", {}).unwrap();
  }
}