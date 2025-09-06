use anyhow::Result;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection, cmd, streams::StreamReadReply};

pub struct RedisManager {
    pub connection: MultiplexedConnection,
    pub publisher: MultiplexedConnection,
}

impl RedisManager {
    pub async fn new() -> Result<Self> {
        let client = Client::open("redis://127.0.0.1/")?;
        let connection = client.get_multiplexed_async_connection().await?;
        let publisher = client.get_multiplexed_async_connection().await?;

        Ok(Self {
            connection,
            publisher,
        })
    }

    pub async fn create_consumer_group(
        &mut self,
        stream: &str,
        group: &str,
        _consumer: &str,
    ) -> Result<()> {
        // cmd! macro for XGROUP CREATE
        let result: Result<String, redis::RedisError> = cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut self.connection)
            .await;

        // Handle the case where the group already exists
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                let err_msg = format!("{}", err);
                if err_msg.contains("BUSYGROUP") {
                    // Consumer group already exists, treat as success
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Redis error: {}", err))
                }
            }
        }
    }

    pub async fn read_stream(
        &mut self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<StreamReadReply> {
        //  cmd! macro for XREADGROUP
        let reply: StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(1000u64)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut self.connection)
            .await?;

        Ok(reply)
    }

    pub async fn acknowledge(&mut self, stream: &str, group: &str, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut cmd = cmd("XACK");
        cmd.arg(stream).arg(group);
        for id in ids {
            cmd.arg(id);
        }

        let _: i32 = cmd.query_async(&mut self.connection).await?;
        Ok(())
    }

    pub async fn publish_response(&mut self, channel: &str, data: &str) -> Result<()> {
        let _: i32 = self.publisher.publish(channel, data).await?;
        Ok(())
    }
}
