use std::collections::HashMap;

use bitcoin::OutPoint;
use mysql::{from_row, params, Pool, PooledConn};
use mysql::prelude::{BinQuery, Queryable, WithParams};
use once_cell::sync::OnceCell;

use crate::index::entry::{Entry, OutPointValue, SatRange};
use crate::SatPoint;

pub struct MBlock {
  pub hash: String,
  pub height: u64,
}

pub struct MOutpoint {
  pub id: u64,
  pub txid: String,
  pub vout: String,
}

pub struct MSatRange {
  pub r0: u64,
  pub r1: u64,
}

static DB_POOL: OnceCell<Pool> = OnceCell::new();

pub struct DB {}

impl DB {
  pub fn new(mysql_url: String) -> Self {
    /// create mysql connection pool
    ///
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

  pub fn insert_outpoint(&self, txid: String, vout: u32) -> u64{
    let mut conn = self.get_connection();
    conn.exec_drop("insert into outpoints (txid, vout) values (:txid, :vout)", params! {"txid" => txid, "vout" => vout}).unwrap();
    conn.last_insert_id()
  }

  pub fn get_outpoint(&self, txid: String, vout: u32) -> Option<u64> {
    let mut conn = self.get_connection();

    let result: Option<u64> = conn.exec_first("select id from outpoints where txid = :txid and vout = :vout", params! {"txid" => txid.clone(), "vout" => vout}).unwrap();
    result
  }

  pub fn get_outpoint_to_sat_ranges(&self, outpointvalue: &OutPointValue) -> Vec<u8> {
    log::trace!("get_outpoint_to_sat_ranges");

    let mut conn = self.get_connection();
    let outpoint: OutPoint = Entry::load(*outpointvalue);
    let outpoint_txid = outpoint.txid.to_string();
    let outpoint_vout = outpoint.vout;


    let sat_ranges: Vec<u8> = conn.exec_first("select unhex(sat_ranges) from outpoints where txid = :txid and vout = :vout", params! {
      "txid" => outpoint_txid,
      "vout" => outpoint_vout
    }).unwrap().unwrap();

    sat_ranges
  }

  pub fn insert_outpoint_to_sat_ranges(&self, outpointvalue: &OutPointValue, sats: &Vec<u8>) {
    log::trace!("insert_outpoint_to_sat_ranges");

    let mut conn = self.get_connection();
    let outpoint: OutPoint = Entry::load(*outpointvalue);
    let outpoint_txid = outpoint.txid.to_string();
    let outpoint_vout = outpoint.vout;

    conn.exec_drop("insert into outpoints (txid, vout, sat_ranges) values (:txid, :vout, :sat_ranges) on duplicate key update sat_ranges = :sat_ranges",
                   params! {"txid" => outpoint_txid, "outpoint_vout" => sats, "sat_ranges" => hex::encode(sats)}).unwrap();
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

  pub fn insert_sat_to_satpoint(&self, start: &u64, sat_point: &SatPoint) {
    log::trace!("insert_sat_to_satpoint");
    let mut conn = self.get_connection();
    let outpoint_txid = sat_point.outpoint.txid.to_string();
    let outpoint_vout = sat_point.outpoint.vout;

    let result = self.get_outpoint(outpoint_txid.clone(), outpoint_vout);
    let outpoint_id = match result {
      None => {
        self.insert_outpoint(outpoint_txid.clone(), outpoint_vout)
      }
      Some(outpoint_id) => {
        outpoint_id
      }
    };

    conn.exec_drop("insert into sat_points (sat, outpoint_id, offset) values (:sat, :outpoint_id, :offset) on duplicate key update outpoint_id = :outpoint_id, offset = :offset", params! {"sat" => start, "outpoint_id" => outpoint_id, "offset" => sat_point.offset}).unwrap();
  }

  pub fn truncate(&self, table_name: String) {
    log::trace!("truncate table {}", table_name.clone());
    let mut conn = self.get_connection();
    // "truncate ?".with((table_name, )).run(conn).unwrap();
    conn.exec_drop("truncate blocks", {}).unwrap();
  }
}