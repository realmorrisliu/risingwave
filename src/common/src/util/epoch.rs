// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use parse_display::Display;

use crate::types::{ScalarImpl, Timestamptz};

static UNIX_RISINGWAVE_DATE_SEC: u64 = 1_617_235_200;

/// [`UNIX_RISINGWAVE_DATE_EPOCH`] represents the risingwave date of the UNIX epoch:
/// 2021-04-01T00:00:00Z.
pub static UNIX_RISINGWAVE_DATE_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| SystemTime::UNIX_EPOCH + Duration::from_secs(UNIX_RISINGWAVE_DATE_SEC));

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

/// `INVALID_EPOCH` defines the invalid epoch value.
pub const INVALID_EPOCH: u64 = 0;

const EPOCH_PHYSICAL_SHIFT_BITS: u8 = 16;

impl Epoch {
    pub fn now() -> Self {
        Self(Self::physical_now() << EPOCH_PHYSICAL_SHIFT_BITS)
    }

    #[must_use]
    pub fn next(self) -> Self {
        let physical_now = Epoch::physical_now();
        let prev_physical_time = self.physical_time();

        // The last 16 bits of the previous epoch ((prev_epoch + 1, prev_epoch + 65536)) will be
        // used as the gap epoch when the mem table spill occurs.
        let next_epoch = match physical_now.cmp(&prev_physical_time) {
            Ordering::Greater => Self::from_physical_time(physical_now),
            Ordering::Equal => {
                tracing::warn!("New generate epoch is too close to the previous one.");
                Epoch(self.0 + 1)
            }
            Ordering::Less => {
                tracing::warn!(
                    "Clock goes backwards when calling Epoch::next(): prev={}, curr={}",
                    prev_physical_time,
                    physical_now
                );
                Epoch(self.0 + 1)
            }
        };

        assert!(next_epoch.0 > self.0);
        next_epoch
    }

    pub fn physical_time(&self) -> u64 {
        self.0 >> EPOCH_PHYSICAL_SHIFT_BITS
    }

    pub fn from_physical_time(time: u64) -> Self {
        Epoch(time << EPOCH_PHYSICAL_SHIFT_BITS)
    }

    pub fn from_unix_millis(mi: u64) -> Self {
        Epoch((mi - UNIX_RISINGWAVE_DATE_SEC * 1000) << EPOCH_PHYSICAL_SHIFT_BITS)
    }

    pub fn physical_now() -> u64 {
        UNIX_RISINGWAVE_DATE_EPOCH
            .elapsed()
            .expect("system clock set earlier than risingwave date!")
            .as_millis() as u64
    }

    pub fn as_unix_millis(&self) -> u64 {
        UNIX_RISINGWAVE_DATE_SEC * 1000 + self.physical_time()
    }

    /// Returns the epoch in a Timestamptz.
    pub fn as_timestamptz(&self) -> Timestamptz {
        Timestamptz::from_millis(self.as_unix_millis() as i64).expect("epoch is out of range")
    }

    /// Returns the epoch in a Timestamptz scalar.
    pub fn as_scalar(&self) -> ScalarImpl {
        self.as_timestamptz().into()
    }

    /// Returns the epoch in real system time.
    pub fn as_system_time(&self) -> SystemTime {
        *UNIX_RISINGWAVE_DATE_EPOCH + Duration::from_millis(self.physical_time())
    }

    /// Returns the epoch subtract `relative_time_ms`, which used for ttl to get epoch corresponding
    /// to the lowerbound timepoint (`src/storage/src/hummock/iterator/forward_user.rs`)
    pub fn subtract_ms(&self, relative_time_ms: u64) -> Self {
        let physical_time = self.physical_time();

        if physical_time < relative_time_ms {
            Epoch(INVALID_EPOCH)
        } else {
            Epoch((physical_time - relative_time_ms) << EPOCH_PHYSICAL_SHIFT_BITS)
        }
    }
}

pub const EPOCH_AVAILABLE_BITS: u64 = 16;
pub const MAX_SPILL_TIMES: u16 = ((1 << EPOCH_AVAILABLE_BITS) - 1) as u16;
pub const EPOCH_MASK: u64 = (1 << EPOCH_AVAILABLE_BITS) - 1;
pub const MAX_EPOCH: u64 = u64::MAX & !EPOCH_MASK;
impl From<u64> for Epoch {
    fn from(epoch: u64) -> Self {
        Self(epoch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EpochPair {
    pub curr: u64,
    pub prev: u64,
}

impl EpochPair {
    pub fn new(curr: u64, prev: u64) -> Self {
        assert!(curr > prev);
        Self { curr, prev }
    }

    pub fn inc(&mut self) {
        self.curr += 1;
        self.prev += 1;
    }

    pub fn new_test_epoch(curr: u64) -> Self {
        assert!(curr > 0);
        Self::new(curr, curr - 1)
    }
}

/// Task-local storage for the epoch pair.
pub mod task_local {
    use futures::Future;
    use tokio::task_local;

    use super::{Epoch, EpochPair};

    task_local! {
        static TASK_LOCAL_EPOCH_PAIR: EpochPair;
    }

    /// Retrieve the current epoch from the task local storage.
    ///
    /// This value is updated after every yield of the barrier message. Returns `None` if the first
    /// barrier message is not yielded.
    pub fn curr_epoch() -> Option<Epoch> {
        TASK_LOCAL_EPOCH_PAIR.try_with(|e| Epoch(e.curr)).ok()
    }

    /// Retrieve the previous epoch from the task local storage.
    ///
    /// This value is updated after every yield of the barrier message. Returns `None` if the first
    /// barrier message is not yielded.
    pub fn prev_epoch() -> Option<Epoch> {
        TASK_LOCAL_EPOCH_PAIR.try_with(|e| Epoch(e.prev)).ok()
    }

    /// Retrieve the epoch pair from the task local storage.
    ///
    /// This value is updated after every yield of the barrier message. Returns `None` if the first
    /// barrier message is not yielded.
    pub fn epoch() -> Option<EpochPair> {
        TASK_LOCAL_EPOCH_PAIR.try_with(|e| *e).ok()
    }

    /// Provides the given epoch pair in the task local storage for the scope of the given future.
    pub async fn scope<F>(epoch: EpochPair, f: F) -> F::Output
    where
        F: Future,
    {
        TASK_LOCAL_EPOCH_PAIR.scope(epoch, f).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone, Utc};

    use super::*;

    #[test]
    fn test_risingwave_system_time() {
        let utc = Utc.with_ymd_and_hms(2021, 4, 1, 0, 0, 0).unwrap();
        let risingwave_dt = Local.from_utc_datetime(&utc.naive_utc());
        let risingwave_st = SystemTime::from(risingwave_dt);
        assert_eq!(risingwave_st, *UNIX_RISINGWAVE_DATE_EPOCH);
    }

    #[test]
    fn test_epoch_generate() {
        let mut prev_epoch = Epoch::now();
        for _ in 0..1000 {
            let epoch = prev_epoch.next();
            assert!(epoch > prev_epoch);
            prev_epoch = epoch;
        }
    }

    #[test]
    fn test_subtract_ms() {
        {
            let epoch = Epoch(10);
            assert_eq!(0, epoch.physical_time());
            assert_eq!(0, epoch.subtract_ms(20).0);
        }

        {
            let epoch = Epoch::now();
            let physical_time = epoch.physical_time();
            let interval = 10;

            assert_ne!(0, physical_time);
            assert_eq!(
                physical_time - interval,
                epoch.subtract_ms(interval).physical_time()
            );
        }
    }
}
