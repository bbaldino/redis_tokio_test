use std::collections::HashMap;

use rand::prelude::*;

use redis::{
    aio::ConnectionLike,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands,
};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub enum WorkerMsg {
    AddSubscriber { id: String },
    RemoveSubscriber { id: String },
}

struct Worker<ConnType> {
    msg_channel: Receiver<WorkerMsg>,
    connection: ConnType,
    subscribers: HashMap<String, String>,
}

impl<T> Worker<T>
where
    T: AsyncCommands + Clone,
{
    async fn run(&mut self) {
        let xread_options = StreamReadOptions::default().block(0).count(1);
        let mut stream_keys: Vec<String> = vec![];
        let mut stream_ids: Vec<String> = vec![];
        let mut i = 0;
        loop {
            i += 1;
            stream_keys.clear();
            stream_ids.clear();
            for (key, val) in self.subscribers.iter() {
                stream_keys.push(key.to_owned());
                stream_ids.push(val.clone());
            }
            println!(
                "[{i}] worker calling select: \n[{i}] keys: {:?}\n[{i}] ids:  {:?}",
                stream_keys, stream_ids
            );
            tokio::select! {
                worker_msg = self.msg_channel.recv() => {
                    println!("[{}] got a worker msg", i);
                    match worker_msg {
                        Some(WorkerMsg::AddSubscriber { id }) => {
                            println!("[{}] Adding subscriber {}", i, id);
                            self.subscribers.insert(id, "0".to_owned());
                        }
                        Some(WorkerMsg::RemoveSubscriber { id }) => {
                            println!("[{}] Removing subscriber {}", i, id);
                            self.subscribers.remove(&id);
                        }
                        None => {}
                    }
                }
                stream_read = self.connection.xread_options::<String, String, StreamReadReply>(&stream_keys, &stream_ids, &xread_options), if stream_keys.len() > 0 => {
                    if let Ok(stream_msg) = stream_read {
                        for stream_key in stream_msg.keys {
                            for stream_id in stream_key.ids {
                                println!("[{}] Received stream message {} on key {}: {:?}", i, stream_id.id, stream_key.key, stream_id);
                                if let Some(last_read_msg_id) = self.subscribers.get_mut(&stream_key.key) {
                                    *last_read_msg_id = stream_id.id;
                                } else {
                                    println!("no subscriber found for {}", stream_key.key);
                                }
                            }
                        }
                    } else {
                        println!("msg: {:?}", stream_read);
                    }
                }
            }
        }
    }
}

struct TestHarness<T> {
    connection: T,
    worker_tx: Sender<WorkerMsg>,
}

impl<T> TestHarness<T>
where
    T: ConnectionLike + Send,
{
    async fn add_subscriber(&mut self) -> String {
        let subscriber_id: String = random::<u32>().to_string();
        self.worker_tx
            .send(WorkerMsg::AddSubscriber {
                id: subscriber_id.clone(),
            })
            .await
            .unwrap();

        subscriber_id
    }

    async fn send_msg(&mut self, id: &str, msg: &str) {
        self.connection
            .xadd::<&str, &str, &str, &str, ()>(&id, "*", &[("data", msg)])
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        //.with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting global default tracing subscriber");
    let client = redis::Client::open("redis://172.17.0.3").unwrap();
    let worker_connection = client.get_multiplexed_tokio_connection().await.unwrap();

    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(1);
    let mut worker = Worker {
        msg_channel: worker_rx,
        connection: worker_connection,
        subscribers: HashMap::new(),
    };

    let worker_task = tokio::spawn(async move {
        worker.run().await;
    });

    let connection = client.get_tokio_connection().await.unwrap();
    let mut harness = TestHarness {
        connection,
        worker_tx,
    };

    let _ = harness.add_subscriber().await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let id_2 = harness.add_subscriber().await;

    harness.send_msg(id_2.as_str(), "hello2").await;
    worker_task.await.unwrap();
}
