use anyhow::Result;
use redis::{
    AsyncCommands, Client,
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
};

pub struct RedisManager {
    pub connection: MultiplexedConnection,
}

impl RedisManager {
    pub async fn new() -> Result<Self> {
        let client = Client::open("redis://127.0.0.1/")?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self { connection })
    }

    pub async fn read_stream(&mut self, stream: &str, last_id: &str) -> Result<StreamReadReply> {
        let opts = StreamReadOptions::default().block(1000).count(10);

        let reply: StreamReadReply = self
            .connection
            .xread_options(&[stream], &[last_id], &opts)
            .await?;

        Ok(reply)
    }

    pub async fn add_to_stream(&mut self, stream: &str, data: &str) -> Result<()> {
        let _: String = self.connection.xadd(stream, "*", &[("data", data)]).await?;
        Ok(())
    }
}
