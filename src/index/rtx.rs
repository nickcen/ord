use super::*;

pub(crate) struct Rtx<'a>(pub(crate) redb::ReadTransaction<'a>);

impl Rtx<'_> {
  pub(crate) fn height(&self) -> Result<Option<Height>> {
    Ok(
      self
        .0
        .open_table(HEIGHT_TO_BLOCK_HASH)?
        .range(0..)?
        .rev()
        .next()
        .map(|(height, _hash)| Height(height.value())),
    )
  }

  pub(crate) fn block_count(&self) -> Result<u64> {
    Ok(
      self
        .0
        .open_table(HEIGHT_TO_BLOCK_HASH)?
        .range(0..)?
        .rev()
        .next()
        .map(|(height, _hash)| height.value() + 1)
        .unwrap_or(0),
    )
  }

  pub(crate) fn export(&self) -> Result {
    println!("HEIGHT_TO_BLOCK_HASH");
    self.0.open_table(HEIGHT_TO_BLOCK_HASH)?.range(0..)?.rev().take(10).for_each(|(height, hash)| {
      println!("height is {}, hash is {}", height.value(), BlockHash::from_inner(*hash.value()));
    });

    println!("INSCRIPTION_ID_TO_INSCRIPTION_ENTRY");
    self.0.open_table(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("key is {:?}, value is {:?}", key.value(), value.value());
    });

    println!("INSCRIPTION_ID_TO_SATPOINT");
    self.0.open_table(INSCRIPTION_ID_TO_SATPOINT)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("height is {:?}, hash is {:?}", key.value(), value.value());
    });

    println!("INSCRIPTION_NUMBER_TO_INSCRIPTION_ID");
    self.0.open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("key is {:?}, value is {:?}", key.value(), value.value());
    });

    // println!("OUTPOINT_TO_SAT_RANGES");
    //
    // self.0.open_table(OUTPOINT_TO_SAT_RANGES)?.iter()?.rev().take(10).for_each(|(key, value)| {
    //   println!("OutPoint is {:?}", OutPoint::load(*key.value()));
    //
    //   for chunk in value.value().chunks_exact(11) {
    //     let (start, end) = SatRange::load(chunk.try_into().unwrap());
    //
    //     println!("start {}, end {}", start, end);
    //   }
    // });

    println!("OUTPOINT_TO_VALUE");
    self.0.open_table(OUTPOINT_TO_VALUE)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("OutPoint is {:?}, value is {:?}", OutPoint::load(*key.value()), value.value());
    });

    println!("SATPOINT_TO_INSCRIPTION_ID");
    self.0.open_table(SATPOINT_TO_INSCRIPTION_ID)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("SatPoint is {:?}, value is {:?}", SatPoint::load(*key.value()), InscriptionId::load(*value.value()));
    });

    println!("SAT_TO_SATPOINT");
    self.0.open_table(SAT_TO_SATPOINT)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("sat is {:?}, SatPoint is {:?}", key.value(), SatPoint::load(*value.value()));
    });

    println!("STATISTIC_TO_COUNT");
    self.0.open_table(STATISTIC_TO_COUNT)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("key is {:?}, value is {:?}", key.value(), value.value());
    });

    println!("WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP");
    self.0.open_table(WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)?.iter()?.rev().take(10).for_each(|(key, value)| {
      println!("key is {:?}, value is {:?}", key.value(), value.value());
    });

    Ok(())
  }
}
