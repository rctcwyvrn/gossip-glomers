use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone, Default)]
struct Inner {
    ids: HashSet<u64>,
    nbrs: Vec<String>,
}

impl Handler {
    fn add(&self, element: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.ids.contains(&element) {
            inner.ids.insert(element);
            return true;
        }
        return false;
    }

    fn get_data(&self) -> Vec<u64> {
        self.inner.lock().unwrap().ids.iter().copied().collect()
    }

    fn update_topology(&self, topology: HashMap<String, Vec<String>>, id: &str) {
        self.inner.lock().unwrap().nbrs = topology.get(id).unwrap().clone()
    }

    fn send_to_nbrs(&self, element: u64, runtime: &Runtime) {
        for dest in self.inner.lock().unwrap().nbrs.iter() {
            runtime.call_async(dest, Request::Broadcast { message: element });
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Read {}) => {
                let data = self.get_data();
                let msg = Request::ReadOk { messages: data };
                return runtime.reply(req, msg).await;
            }
            Ok(Request::Broadcast { message: element }) => {
                if self.add(element) {
                    self.send_to_nbrs(element, &runtime);
                }
                return runtime.reply_ok(req).await;
            }
            Ok(Request::Topology { topology }) => {
                self.update_topology(topology, runtime.node_id());
                return runtime.reply_ok(req).await;
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Read {},
    ReadOk {
        messages: Vec<u64>,
    },
    Broadcast {
        message: u64,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}
