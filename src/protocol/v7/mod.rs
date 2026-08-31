/// `pub(super)` fence (U2 / Task 7.1): `frames` is reachable ONLY from inside
/// the `protocol` module family — `protocol::wire` and this codec. Any encode
/// or decode elsewhere in the crate (or in benches / integration tests, which
/// are separate crates) MUST go through `protocol::wire` / a `Codec`, and a
/// direct `v7::frames::…` call there is a COMPILE ERROR, so no new direct
/// caller can silently appear.
pub(super) mod frames;

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
