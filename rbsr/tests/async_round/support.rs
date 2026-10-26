use std::future::{poll_fn, Future};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use rbsr::{AsyncRsosView, EnumerationRange, RangeAggregate, RsosView};
use rsos::{Aggregate, Fingerprint, FingerprintTreeMap};

pub type Map = FingerprintTreeMap<i32, i32>;
pub type Outputs = (Vec<RangeAggregate<i32>>, Vec<EnumerationRange<i32>>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    Size,
    Aggregate(Bound<i32>, Bound<i32>),
    Rank(i32),
    Select(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Io,
    Expired,
}

pub struct View {
    pub root: Arc<Map>,
    pub trace: Mutex<Vec<Query>>,
    pub yields: bool,
    pub fail: Option<usize>,
    pub expired: AtomicBool,
    pub hostile: bool,
}

impl View {
    pub fn new(root: Arc<Map>, yields: bool) -> Self {
        Self {
            root,
            trace: Mutex::new(Vec::new()),
            yields,
            fail: None,
            expired: AtomicBool::new(false),
            hostile: false,
        }
    }

    pub fn trace(&self) -> Vec<Query> {
        self.trace.lock().unwrap().clone()
    }

    fn record(&self, query: Query) -> usize {
        let mut trace = self.trace.lock().unwrap();
        trace.push(query);
        trace.len() - 1
    }

    async fn query(&self, query: Query) -> Result<(), Error> {
        let ordinal = self.record(query);
        let mut yielded = !self.yields;
        poll_fn(|cx| {
            if !yielded {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        if self.expired.load(Ordering::SeqCst) {
            Err(Error::Expired)
        } else if self.fail == Some(ordinal) {
            Err(Error::Io)
        } else {
            Ok(())
        }
    }
}

fn bounds<R: RangeBounds<i32>>(range: R) -> (Bound<i32>, Bound<i32>) {
    (range.start_bound().cloned(), range.end_bound().cloned())
}

impl RsosView<i32> for View {
    fn size(&self) -> usize {
        self.record(Query::Size);
        self.root.len()
    }
    fn aggregate<R: RangeBounds<i32>>(&self, range: R) -> Aggregate {
        let range = bounds(range);
        self.record(Query::Aggregate(range.0, range.1));
        self.root.aggregate(range)
    }
    fn rank(&self, key: &i32) -> usize {
        self.record(Query::Rank(*key));
        if self.hostile {
            usize::MAX
        } else {
            self.root.rank(key)
        }
    }
    fn select(&self, rank: usize) -> &i32 {
        self.record(Query::Select(rank));
        self.root.select(rank)
    }
}

impl AsyncRsosView<i32> for View {
    type Error = Error;
    async fn size(&self) -> Result<usize, Error> {
        self.query(Query::Size).await?;
        Ok(self.root.len())
    }
    async fn aggregate(&self, range: (Bound<i32>, Bound<i32>)) -> Result<Aggregate, Error> {
        self.query(Query::Aggregate(range.0, range.1)).await?;
        Ok(self.root.aggregate(range))
    }
    async fn rank(&self, key: &i32) -> Result<usize, Error> {
        self.query(Query::Rank(*key)).await?;
        Ok(if self.hostile {
            usize::MAX
        } else {
            self.root.rank(key)
        })
    }
    async fn select(&self, rank: usize) -> Result<i32, Error> {
        self.query(Query::Select(rank)).await?;
        Ok(*self.root.select(rank))
    }
}

pub fn waker() -> Waker {
    Waker::noop().clone()
}
pub fn run<F: Future>(future: F) -> F::Output {
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    for _ in 0..100_000 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
    panic!("fixture did not complete");
}
pub fn root() -> Arc<Map> {
    Arc::new((0..100).map(|key| (key, key * 2)).collect())
}
pub fn active() -> Vec<RangeAggregate<i32>> {
    let remote = Aggregate::new(99, Fingerprint([1, 2, 3, 4]));
    vec![
        RangeAggregate::new(Some(3), Some(75), remote),
        RangeAggregate::new(Some(80), None, Aggregate::ZERO),
        RangeAggregate::new(Some(90), Some(10), remote),
        RangeAggregate::new(None, Some(1), remote),
    ]
}
pub fn outputs() -> Outputs {
    (
        vec![RangeAggregate::new(Some(-10), Some(-5), Aggregate::ZERO)],
        vec![(Bound::Included(-20), Bound::Excluded(-15))],
    )
}
