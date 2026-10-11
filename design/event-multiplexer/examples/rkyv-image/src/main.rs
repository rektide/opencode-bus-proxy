//! Codec experiment only; does not freeze or restore gateway runtime objects.
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
struct BlobRange {
    offset: u64,
    length: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
struct SubscriptionImage {
    id: String,
    active: bool,
    sessions: Vec<String>,
    frames: Vec<BlobRange>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
struct Image {
    schema_version: u32,
    generation: u64,
    subscriptions: Vec<SubscriptionImage>,
    arena: Vec<u8>,
}

fn main() -> Result<(), Error> {
    let frame = b"data: {\"type\":\"demo\"}\n\n";
    let image = Image {
        schema_version: 1,
        generation: 7,
        subscriptions: vec![SubscriptionImage {
            id: "evsub_demo".into(),
            active: true,
            sessions: vec!["a".into()],
            frames: vec![BlobRange {
                offset: 0,
                length: frame.len() as u64,
            }],
        }],
        arena: frame.to_vec(),
    };
    let bytes = rkyv::to_bytes::<Error>(&image)?;
    let archived = rkyv::access::<ArchivedImage, Error>(&bytes)?;
    assert_eq!(archived.schema_version.to_native(), 1);
    let restored = rkyv::from_bytes::<Image, Error>(&bytes)?;
    assert_eq!(restored, image);
    // bytecheck validates the archive representation, not application references.
    for sub in &restored.subscriptions {
        for range in &sub.frames {
            let end = range
                .offset
                .checked_add(range.length)
                .expect("range overflow");
            assert!(end <= restored.arena.len() as u64);
        }
    }
    // An obviously truncated archive must be rejected by the checked API.
    assert!(rkyv::access::<ArchivedImage, Error>(&bytes[..1]).is_err());
    println!("validated {} bytes; exact frame preserved", bytes.len());
    Ok(())
}
