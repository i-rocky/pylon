pub mod frames;

use crate::protocol::codec::{Capabilities, Codec, DecodeError};
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;

#[derive(Debug)]
pub struct V7Codec;

impl Codec for V7Codec {
    fn version(&self) -> u8 {
        7
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            client_events: true,
            presence: true,
        }
    }
    fn decode(&self, text: &str) -> Result<ClientCommand, DecodeError> {
        frames::decode(text)
    }
    /// The encoding seam (F6 / Task 6.4): append-only, `Raw` payloads shared
    /// by reference. `encode` is inherited from the trait default, which
    /// delegates here — both paths stay byte-identical by construction.
    fn encode_into(&self, event: &ServerEvent, out: &mut String) {
        frames::encode_into(event, out)
    }
}
