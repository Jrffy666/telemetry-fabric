# Rust Producer Skeleton

This is a wiring guide for Rust services that publish contract envelopes. Use
generated protobuf types from a shared contracts crate and keep Kafka client
choice local to the service.

```rust
pub struct KafkaMessage {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

pub trait KafkaProducer {
    fn send(&self, message: KafkaMessage) -> Result<(), ProducerError>;
}

#[derive(Debug)]
pub struct ProducerError(pub String);

pub struct EnvelopeMeta<'a> {
    pub tenant_id: &'a str,
    pub event_type: &'a str,
    pub schema_version: &'a str,
    pub priority: &'a str,
    pub dedupe_key: &'a str,
}

pub fn publish_envelope<P: KafkaProducer>(
    producer: &P,
    topic: &str,
    key: Vec<u8>,
    encoded_envelope: Vec<u8>,
    meta: EnvelopeMeta<'_>,
) -> Result<(), ProducerError> {
    producer.send(KafkaMessage {
        topic: topic.to_owned(),
        key,
        value: encoded_envelope,
        headers: vec![
            ("tenant_id".to_owned(), meta.tenant_id.as_bytes().to_vec()),
            ("event_type".to_owned(), meta.event_type.as_bytes().to_vec()),
            ("schema_version".to_owned(), meta.schema_version.as_bytes().to_vec()),
            ("priority".to_owned(), meta.priority.as_bytes().to_vec()),
            ("dedupe_key".to_owned(), meta.dedupe_key.as_bytes().to_vec()),
        ],
    })
}
```

The producer should retry transient broker errors locally. Schema or decode
failures should not be retried blindly; publish contextual failures to
`chain.events.dead_letter`.
