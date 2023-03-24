#![allow(
  clippy::too_many_arguments,
  clippy::type_complexity,
  clippy::result_large_err
)]
#![deny(
  clippy::cast_lossless,
  clippy::cast_possible_truncation,
  clippy::cast_possible_wrap,
  clippy::cast_sign_loss
)]

use {
  self::{
    arguments::Arguments,
    blocktime::Blocktime,
    config::Config,
    decimal::Decimal,
    degree::Degree,
    deserialize_from_str::DeserializeFromStr,
    epoch::Epoch,
    height::Height,
    index::{Index, List},
    inscription::Inscription,
    inscription_id::InscriptionId,
    media::Media,
    options::Options,
    outgoing::Outgoing,
    representation::Representation,
    subcommand::Subcommand,
    tally::Tally,
  },
  anyhow::{anyhow, bail, Context, Error},
  bip39::Mnemonic,
  bitcoin::{
    blockdata::constants::COIN_VALUE,
    consensus::{self, Decodable, Encodable},
    hash_types::BlockHash,
    hashes::Hash,
    Address, Amount, Block, Network, OutPoint, Script, Sequence, Transaction, TxIn, TxOut, Txid,
  },
  bitcoincore_rpc::{Client, RpcApi},
  chain::Chain,
  chrono::{DateTime, TimeZone, Utc},
  clap::{ArgGroup, Parser},
  derive_more::{Display, FromStr},
  html_escaper::{Escape, Trusted},
  lazy_static::lazy_static,
  regex::Regex,
  serde::{Deserialize, Deserializer, Serialize, Serializer},
  std::{
    cmp,
    collections::{BTreeMap, HashSet, VecDeque},
    env,
    ffi::OsString,
    fmt::{self, Display, Formatter},
    fs::{self, File},
    io,
    net::{TcpListener, ToSocketAddrs},
    ops::{Add, AddAssign, Sub},
    path::{Path, PathBuf},
    process::{self, Command},
    str::FromStr,
    sync::{
      atomic::{self, AtomicU64},
      Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime},
  },
  tempfile::TempDir,
  tokio::{runtime::Runtime, task},
};

pub use crate::{
  fee_rate::FeeRate, object::Object, rarity::Rarity, sat::Sat, sat_point::SatPoint,
  subcommand::wallet::transaction_builder::TransactionBuilder,
};

#[cfg(test)]
#[macro_use]
mod test;

#[cfg(test)]
use self::test::*;

macro_rules! tprintln {
    ($($arg:tt)*) => {

      if cfg!(test) {
        eprint!("==> ");
        eprintln!($($arg)*);
      }
    };
}

mod arguments;
mod blocktime;
mod chain;
mod config;
mod decimal;
mod degree;
mod deserialize_from_str;
mod epoch;
mod fee_rate;
mod height;
mod index;
mod inscription;
mod inscription_id;
mod media;
mod object;
mod options;
mod outgoing;
mod page_config;
mod rarity;
mod representation;
mod sat;
mod sat_point;
pub mod subcommand;
mod tally;
mod templates;
mod wallet;

type Result<T = (), E = Error> = std::result::Result<T, E>;

const DIFFCHANGE_INTERVAL: u64 = bitcoin::blockdata::constants::DIFFCHANGE_INTERVAL as u64;
const SUBSIDY_HALVING_INTERVAL: u64 =
  bitcoin::blockdata::constants::SUBSIDY_HALVING_INTERVAL as u64;
const CYCLE_EPOCHS: u64 = 6;

static INTERRUPTS: AtomicU64 = AtomicU64::new(0);
static LISTENERS: Mutex<Vec<axum_server::Handle>> = Mutex::new(Vec::new());

fn integration_test() -> bool {
  env::var_os("ORD_INTEGRATION_TEST")
    .map(|value| value.len() > 0)
    .unwrap_or(false)
}

fn timestamp(seconds: u32) -> DateTime<Utc> {
  Utc.timestamp_opt(seconds.into(), 0).unwrap()
}

const INTERRUPT_LIMIT: u64 = 5;

pub fn main() {
  env_logger::init();

  ctrlc::set_handler(move || {
    LISTENERS
      .lock()
      .unwrap()
      .iter()
      .for_each(|handle| handle.graceful_shutdown(Some(Duration::from_millis(100))));

    println!("Detected Ctrl-C, attempting to shut down ord gracefully. Press Ctrl-C {INTERRUPT_LIMIT} times to force shutdown.");

    let interrupts = INTERRUPTS.fetch_add(1, atomic::Ordering::Relaxed);

    if interrupts > INTERRUPT_LIMIT {
      process::exit(1);
    }
  })
  .expect("Error setting ctrl-c handler");

  if let Err(err) = Arguments::parse().run() {
    eprintln!("error: {err}");
    err
      .chain()
      .skip(1)
      .for_each(|cause| eprintln!("because: {cause}"));
    if env::var_os("RUST_BACKTRACE")
      .map(|val| val == "1")
      .unwrap_or_default()
    {
      eprintln!("{}", err.backtrace());
    }
    process::exit(1);
  }
}

pub fn debug() {
  env_logger::init();

  let options = Options{
    bitcoin_data_dir: None,
    chain_argument: Default::default(),
    config: None,
    config_dir: None,
    cookie_file: None,
    data_dir: None,
    first_inscription_height: None,
    height_limit: Some(767431),
    index: None,
    index_sats: false,
    regtest: false,
    rpc_url: Some("13.231.161.20:8332".to_string()),
    // rpc_url: Some("192.168.2.191:8332".to_string()),
    mysql_url: "mysql://test:test@localhost:3306/bitcoin".to_string(),
    signet: false,
    testnet: false,
    wallet: "".to_string()
  };

  let index = Index::open(&options).unwrap();

  // index.outputs(); // run outputs
  // index.fetch("140b16c2d3eca31586dd99106f53526011d4e366d38eb64e3c6f601c94758867".to_string()).unwrap(); // run outputs
  // index.enscription("140b16c2d3eca31586dd99106f53526011d4e366d38eb64e3c6f601c94758867".to_string());


  // fs::copy("/Volumes/Personal/Library/Application Support/ord/index_74998.redb", "/Volumes/Personal/Library/Application Support/ord/index.redb").unwrap();
  // fs::remove_file("/Volumes/Personal/Library/Application Support/ord/index.redb").unwrap();
  // index.db.truncate("blocks".to_string());

  index.update().unwrap(); // run index

  // start server
  // let index = Arc::new(Index::open(&options).unwrap());
  // let handle = axum_server::Handle::new();
  // LISTENERS.lock().unwrap().push(handle.clone());
  // let server = subcommand::server::Server{
  //   address: "0.0.0.0".to_string(),
  //   acme_domain: vec![],
  //   http_port: None,
  //   https_port: None,
  //   acme_cache: None,
  //   acme_contact: vec![],
  //   http: false,
  //   https: false,
  //   redirect_http_to_https: false
  // };
  // server.run(options, index, handle).unwrap();
}