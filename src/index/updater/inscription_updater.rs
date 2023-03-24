use super::*;

pub(super) struct Flotsam {
  inscription_id: InscriptionId,
  offset: u64,
  origin: Origin,
}

enum Origin {
  New(u64),
  Old(SatPoint),
}

pub(super) struct InscriptionUpdater<'a> {
  flotsam: Vec<Flotsam>,
  height: u64,
  value_receiver: &'a mut Receiver<u64>,
  lost_sats: u64,
  next_number: u64,
  reward: u64,
  timestamp: u32,
  value_cache: &'a mut HashMap<OutPoint, u64>,
  db: &'a DB
}

impl<'a> InscriptionUpdater<'a> {
  pub(super) fn new(
    height: u64,
    value_receiver: &'a mut Receiver<u64>,
    lost_sats: u64,
    timestamp: u32,
    value_cache: &'a mut HashMap<OutPoint, u64>,
    db: &'a DB
  ) -> Result<Self> {
    /// 找到库里面最大的 number，然后加1
    let next_number = db.get_inscription_next_number();

    Ok(Self {
      flotsam: Vec::new(),
      height,
      value_receiver,
      lost_sats,
      next_number,
      reward: Height(height).subsidy(),
      timestamp,
      value_cache,
      db
    })
  }

  pub(super) fn index_transaction_inscriptions(
    &mut self,
    index: &Index,
    tx: &Transaction,
    txid: Txid,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
  ) -> Result<u64> {
    let mut inscriptions = Vec::new();

    let mut input_value = 0;
    for tx_in in &tx.input {
      if tx_in.previous_output.is_null() {
        input_value += Height(self.height).subsidy();
      } else {
        /// 找 input 对应的 output 看看他们上面有没有 inscription 记录
        /// 假设这个 input 的 previous_output，像这样
        /// [o1, e1], [o2, e2], [o3, e3]
        /// 在将 input 映射到新的 output 的时候，上面的这些区间就会被当成一个大数组处理
        /// input_value 就是将 output 里面的 inscription 的位置，定位到大数组的偏移量。
        /// 从而知道，某个 inscription 会落到哪个新的 output 上
        ///
        // TODO 用 SATPOINT_TO_INSCRIPTION_ID 这张表
        for old_satpoint in self.db.get_sat_point_by_outpoint(&tx_in.previous_output)
          // Index::inscriptions_on_output(self.satpoint_to_id, tx_in.previous_output)?
        {
          if let Some(inscription) = self.db.get_inscription_by_sat_point(&old_satpoint) {
            let out_point = self.db.get_outpoint_by_id(old_satpoint.outpoint_id).unwrap();
            inscriptions.push(Flotsam {
              offset: input_value + old_satpoint.offset,
              inscription_id: InscriptionId{
                txid: Txid::from_str(&inscription.inscription_id)?,
                index: 0
              },
              origin: Origin::Old(SatPoint{ outpoint: OutPoint{
                txid: Txid::from_str(&out_point.txid)?,
                vout: out_point.vout.parse()?
              }, offset: old_satpoint.offset }),
            });
          }

        }

        input_value += if let Some(value) = self.value_cache.remove(&tx_in.previous_output) {
          log::trace!("case 1 in value_cache");
          value
        } else if let Some(value) = index.db.get_outpoint_value(tx_in.previous_output.txid.to_string(), tx_in.previous_output.vout)
        {
          log::trace!("case 2 in outpoint_to_value");
          value
        } else {
          log::trace!("case 3 从 value_receiver 取");
          self.value_receiver.blocking_recv().ok_or_else(|| {
            anyhow!(
              "failed to get transaction for {}",
              tx_in.previous_output.txid
            )
          })?
        }
      }
    }

    /// 这里就是判断第一个 inscription 是不是刻在 0 的位置上，因为后续的 inscription 都有 input_value 做偏移，不会为 0。

    if inscriptions.iter().all(|flotsam| flotsam.offset != 0)
      && Inscription::from_transaction(tx).is_some()
    {
      log::trace!("we are here: ");
      /// Origin::New 里面计算出来的是 fee 值，输入 value 减去 输出 value
      /// 新的这个 inscription 会在 offset 为 0 的位置上。所以，后续在映射到 output 之前，需要先排序。
      inscriptions.push(Flotsam {
        inscription_id: txid.into(),
        offset: 0,
        origin: Origin::New(input_value - tx.output.iter().map(|txout| txout.value).sum::<u64>()),
      });
    };

    /// is_coinbase 就是判断这笔输入的第0笔是不是挖矿奖励，因为挖矿奖励的 previous_output 是没有的。
    /// 不确定 bitcoin 有没有规定，当有多笔输入，且输入包含挖矿奖励时，他需要放在第一个。
    /// 或者这里是用来处理这个 block 的第 0 个 transaction 输入的情况，就是发放挖矿奖励的情况。
    /// 因为 updater 处理 block 的 transactions 的时候，是先处理 1..n，最后处理的 0
    ///
    let is_coinbase = tx
      .input
      .first()
      .map(|tx_in| tx_in.previous_output.is_null())
      .unwrap_or_default();

    /// 对于 is_coinbase 为 true 的情况，inscriptions 里面是空的，因为这是挖矿奖励，由系统发出，没办法往上面加 inscription
    if is_coinbase {
      inscriptions.append(&mut self.flotsam);
    }

    inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = inscriptions.into_iter().peekable();

    let mut output_value = 0;
    /// 将 input 里面的 inscriptions，分配进到对应的 output
    for (vout, tx_out) in tx.output.iter().enumerate() {
      let end = output_value + tx_out.value;

      while let Some(flotsam) = inscriptions.peek() {
        if flotsam.offset >= end {
          break;
        }

        /// 这里是计算新的 inscription 在 outpoint 里面的偏移位置
        let new_satpoint = SatPoint {
          outpoint: OutPoint {
            txid,
            vout: vout.try_into().unwrap(),
          },
          offset: flotsam.offset - output_value,
        };

        self.update_inscription_location(
          index,
          input_sat_ranges,
          inscriptions.next().unwrap(),
          new_satpoint,
        )?;
      }

      output_value = end;

      self.value_cache.insert(
        OutPoint {
          vout: vout.try_into().unwrap(),
          txid,
        },
        tx_out.value,
      );
    }

    /// 我猜这里应该是处理，还剩余部分 inscription 没有进到 output 的情况。
    /// 例如，落到 fee 里面了，也就是给矿工了
    /// 返回的值是丢掉的 sat 数量

    if is_coinbase {
      /// 这里应该是把没处理完的清掉了，都转移到 OutPoint::null() 了
      for flotsam in inscriptions {
        let new_satpoint = SatPoint {
          outpoint: OutPoint::null(),
          offset: self.lost_sats + flotsam.offset - output_value,
        };
        self.update_inscription_location(index, input_sat_ranges, flotsam, new_satpoint)?;
      }

      Ok(self.reward - output_value)
    } else {
      /// 这里相当于把没处理的先暂存起来了
      self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
        offset: self.reward + flotsam.offset,
        ..flotsam
      }));
      self.reward += input_value - output_value;
      Ok(0)
    }
  }

  /// 这个要好好看看
  fn update_inscription_location(
    &mut self,
    index: &Index,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
  ) -> Result {
    // let inscription_id = flotsam.inscription_id.store();

    match flotsam.origin {
      Origin::Old(old_satpoint) => {
        self.db.update_inscription_sat_point(&flotsam.inscription_id, Some(&old_satpoint));
        // self.satpoint_to_id.remove(&old_satpoint.store())?;
      }
      Origin::New(fee) => {
        // self
        //   .number_to_id
        //   .insert(&self.next_number, &inscription_id)?;

        /// 找到这个 inscription 在 sat_range 里面的位置
        /// [s1, e1], [s2, e2], [s3, e3] ......
        /// size 就是 e - s，通过循环，判断 flotsam.offset 落在哪个区间，然后定位到在该区间内的偏移值，算出是编号为多少的 sat

        let mut n: u64 = 0;
        let mut sat = None;
        if let Some(input_sat_ranges) = input_sat_ranges {
          let mut offset = 0;
          for (start, end) in input_sat_ranges {
            let size = end - start;
            if offset + size > flotsam.offset {
              n = start + flotsam.offset - offset;
              sat = Some(Sat(n));
              // self.sat_to_inscription_id.insert(&n, &inscription_id)?;
              break;
            }
            offset += size;
          }
        }

        index.db.insert_inscription(&flotsam.inscription_id, &InscriptionEntry {
          fee,
          height: self.height,
          number: self.next_number,
          sat,
          timestamp: self.timestamp,
        }, &new_satpoint);

        // self.id_to_entry.insert(
        //   &inscription_id,
        //   &InscriptionEntry {
        //     fee,
        //     height: self.height,
        //     number: self.next_number,
        //     sat,
        //     timestamp: self.timestamp,
        //   }
        //   .store(),
        // )?;

        self.next_number += 1;
      }
    }

    // let new_satpoint = new_satpoint.store();
    // self.satpoint_to_id.insert(&new_satpoint, &inscription_id)?;
    // self.id_to_satpoint.insert(&inscription_id, &new_satpoint)?;

    Ok(())
  }
}
