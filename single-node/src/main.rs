use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use std::sync::{Arc, Mutex};
use std::collections::{HashSet, HashMap};
use serde::{Serialize, Deserialize};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    inner : Arc<Mutex<Inner>> 
}

#[derive(Clone, Default)]
struct Inner {
    ids: HashSet<u64>
}

impl Handler {
    fn add(&self, element: u64) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.ids.contains(&element) {
            inner.ids.insert(element); 
        }
        
    }

    fn get_data(&self) -> Vec<u64> {
        self.inner.lock().unwrap().ids.iter().copied().collect()
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
                self.add(element);
                return runtime.reply_ok(req).await;
            }
            Ok(Request::Topology { topology: _ }) => {
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