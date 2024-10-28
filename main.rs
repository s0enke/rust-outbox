extern crate mysql;
extern crate rustc_serialize;

use crate::mysql::prelude::Queryable;
use aws_config::BehaviorVersion;
use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client;
use mysql as my;
use mysql::PooledConn;
use std::error::Error;
use tokio;

const QUEUE_URL: &'static str = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue.fifo"; // fixme: make configurable

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    main_loop().await?;

    Ok(())
}

pub async fn main_loop() -> Result<(), Box<dyn Error>> {
    let sqs_client = setup_sqs().await;

    let mut conn = setup_mysql();
    let query = "SELECT * FROM outbox order by id";
    let result: Vec<my::Row> = conn.query(query).unwrap();

    for row in result {
        let row = row.unwrap();
        let id: i32 = my::from_value(row[0].clone());
        let payload: String = my::from_value(row[1].clone());

        // push the row into aws sqs
        sqs_client
            .send_message()
            .queue_url(QUEUE_URL)
            .message_attributes(
                "id",
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(id.to_string())
                    .build()?,
            )
            .message_body(&payload)
            .send()
            .await?;
    }
    Ok(())
}

pub fn setup_mysql() -> PooledConn {
    let pool = my::Pool::new("mysql://user:password@localhost:3307/db").unwrap(); // fixme: make configurable
    pool.get_conn().unwrap()
}

pub async fn setup_sqs() -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region("us-east-1") // fixme: make configurable
        .test_credentials()
        .endpoint_url("http://localhost:3001") // fixme: make configurable
        .load()
        .await;
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    sqs_client
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn end_to_end_happy_path() {
        let mut mysql_conn = setup_mysql();

        let query = "DROP TABLE IF EXISTS outbox";
        mysql_conn.query_drop(query).unwrap();

        let query = "CREATE TABLE `outbox` (
  `id` int NOT NULL AUTO_INCREMENT,
  `payload` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB";
        mysql_conn.query_drop(query).unwrap();

        let sqs_client = setup_sqs().await;
        if let Err(_e) = sqs_client.delete_queue().queue_url(QUEUE_URL).send().await {
            // ignore error if the queue does not exist
        }
        // create the queue
        sqs_client
            .create_queue()
            .queue_name("MyQueue.fifo")
            .send()
            .await
            .unwrap();

        // insert two rows
        let query = "INSERT INTO outbox (id, payload) VALUES (2, '{\"name\": \"John\"}'), (1, '{\"name\": \"Jane\"}')";
        mysql_conn.query_drop(query).unwrap();

        main_loop().await.unwrap();

        // check the message in the queue
        let messages = sqs_client
            .receive_message()
            .max_number_of_messages(2)
            .message_attribute_names("id")
            .queue_url(QUEUE_URL)
            .send()
            .await
            .unwrap()
            .messages
            .unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0]
                .message_attributes()
                .unwrap()
                .get("id")
                .unwrap()
                .string_value()
                .unwrap(),
            "1"
        );
        assert_eq!(messages[0].body.as_deref(), Some("{\"name\": \"Jane\"}")); // Jane has id 1, so it must be first
        assert_eq!(messages[1].body.as_deref(), Some("{\"name\": \"John\"}"));

        // drop table again
        let query = "DROP TABLE outbox";
        mysql_conn.query_drop(query).unwrap();
    }
}
