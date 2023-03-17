use std::path::PathBuf;
use {
  redb::{Database, ReadableTable, Table, TableDefinition, WriteStrategy, WriteTransaction},
  ord::*
};


macro_rules! define_table {
  ($name:ident, $key:ty, $value:ty) => {
    const $name: TableDefinition<$key, $value> = TableDefinition::new(stringify!($name));
  };
}

define_table! { HEIGHT_TO_BLOCK_HASH, u64, &BlockHashValue }
define_table! { INSCRIPTION_ID_TO_INSCRIPTION_ENTRY, &InscriptionIdValue, InscriptionEntryValue }
define_table! { INSCRIPTION_ID_TO_SATPOINT, &InscriptionIdValue, &SatPointValue }
define_table! { INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, u64, &InscriptionIdValue }
define_table! { OUTPOINT_TO_SAT_RANGES, &OutPointValue, &[u8] }
define_table! { OUTPOINT_TO_VALUE, &OutPointValue, u64}
define_table! { SATPOINT_TO_INSCRIPTION_ID, &SatPointValue, &InscriptionIdValue }
define_table! { SAT_TO_INSCRIPTION_ID, u64, &InscriptionIdValue }
define_table! { SAT_TO_SATPOINT, u64, &SatPointValue }
define_table! { STATISTIC_TO_COUNT, u64, u64 }
define_table! { WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP, u64, u128 }

fn main() {
  println!("hello");

  let mut path = PathBuf::new();
  path.push("/Volumes/Personal/Library/Application Support/ord/index.redb");

  let database = unsafe { Database::builder().open_mmapped(&path).unwrap() };
  let rtx = database.begin_read().unwrap();
  let entry = rtx.open_table(HEIGHT_TO_BLOCK_HASH)?;


  println!("{:?}", database);

}