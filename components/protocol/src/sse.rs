use crate::ProtocolError;
use serde::{Deserialize, Serialize};

/// Exact incremental parser state, versioned by the enclosing handover format.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecoderImage {
    frame_bytes: u64,
    #[serde(with = "crate::buffer")]
    line: Vec<u8>,
    #[serde(with = "crate::buffer")]
    data: Vec<u8>,
    has_data: bool,
    skip_lf: bool,
    #[serde(with = "crate::buffer")]
    prefix: Vec<u8>,
    started: bool,
}

impl DecoderImage {
    /// Private-buffer occupancy, for bounding an enclosing transfer image.
    pub fn retained_bytes(&self) -> usize {
        self.line
            .len()
            .saturating_add(self.data.len())
            .saturating_add(self.prefix.len())
    }
}

/// Incremental SSE data decoder. Each byte is visited a constant number of times,
/// even for one-byte input chunks. The bound counts all non-CR/LF bytes between
/// blank lines, including field prefixes/comments; a leading UTF-8 BOM is exempt.
/// Line/data storage is O(limit). Returned complete frames belong to the caller.
///
/// EOF with pending data or an unterminated non-comment field is an error (the
/// source must never dispatch an unterminated frame). A final comment is harmless.
/// Any error or finish closes the decoder permanently.
pub struct SseDecoder {
    limit: usize,
    frame_bytes: usize,
    line: Vec<u8>,
    data: Vec<u8>,
    has_data: bool,
    skip_lf: bool,
    prefix: Vec<u8>,
    started: bool,
    closed: bool,
}

impl SseDecoder {
    pub fn export(&self) -> Result<DecoderImage, ProtocolError> {
        if self.closed {
            return Err(ProtocolError::DecoderClosed);
        }
        Ok(DecoderImage {
            frame_bytes: self.frame_bytes as u64,
            line: self.line.clone(),
            data: self.data.clone(),
            has_data: self.has_data,
            skip_lf: self.skip_lf,
            prefix: self.prefix.clone(),
            started: self.started,
        })
    }

    pub fn import(image: DecoderImage, limit: usize) -> Result<Self, ProtocolError> {
        // Data has one synthetic LF per data field, so it can exceed the counted
        // value by at most the number of counted field prefixes.
        if image.frame_bytes > limit as u64
            || image.line.len() > limit
            || image.data.len() > limit.saturating_mul(2)
            || image.line.len() as u64 > image.frame_bytes
            || image.prefix.len() > 2
            || (image.started && !image.prefix.is_empty())
            || (!image.started && !b"\xef\xbb\xbf".starts_with(&image.prefix))
            || (!image.has_data && !image.data.is_empty())
        {
            return Err(ProtocolError::InvalidEvent);
        }
        Ok(Self {
            limit,
            frame_bytes: image.frame_bytes as usize,
            line: image.line,
            data: image.data,
            has_data: image.has_data,
            skip_lf: image.skip_lf,
            prefix: image.prefix,
            started: image.started,
            closed: false,
        })
    }

    pub fn new(max_frame_bytes: usize) -> Self {
        Self {
            limit: max_frame_bytes,
            frame_bytes: 0,
            line: Vec::new(),
            data: Vec::new(),
            has_data: false,
            skip_lf: false,
            prefix: Vec::with_capacity(3),
            started: false,
            closed: false,
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
        if self.closed {
            return Err(ProtocolError::DecoderClosed);
        }
        let mut output = Vec::new();
        for &byte in chunk {
            if !self.started {
                self.prefix.push(byte);
                const BOM: &[u8] = b"\xef\xbb\xbf";
                if BOM.starts_with(&self.prefix) {
                    if self.prefix.len() == BOM.len() {
                        self.prefix.clear();
                        self.started = true;
                    }
                    continue;
                }
                self.started = true;
                let prefix = std::mem::take(&mut self.prefix);
                for byte in prefix {
                    if let Err(error) = self.byte(byte, &mut output) {
                        return self.fail(error);
                    }
                }
                continue;
            }
            if let Err(error) = self.byte(byte, &mut output) {
                return self.fail(error);
            }
        }
        Ok(output)
    }

    fn byte(&mut self, byte: u8, output: &mut Vec<Vec<u8>>) -> Result<(), ProtocolError> {
        if self.skip_lf {
            self.skip_lf = false;
            if byte == b'\n' {
                return Ok(());
            }
        }
        if byte == b'\r' || byte == b'\n' {
            self.skip_lf = byte == b'\r';
            return self.end_line(output);
        }
        if self.frame_bytes == self.limit {
            return Err(ProtocolError::FrameTooLarge { limit: self.limit });
        }
        self.frame_bytes += 1;
        self.line.push(byte);
        Ok(())
    }

    fn end_line(&mut self, output: &mut Vec<Vec<u8>>) -> Result<(), ProtocolError> {
        std::str::from_utf8(&self.line).map_err(|_| ProtocolError::InvalidUtf8)?;
        if self.line.is_empty() {
            if self.has_data {
                // A trailing newline is inserted for every data field and then
                // removed on dispatch, as specified by SSE (including empty data).
                self.data.pop();
                output.push(std::mem::take(&mut self.data));
                self.has_data = false;
            }
            self.frame_bytes = 0;
        } else {
            let colon = self.line.iter().position(|&byte| byte == b':');
            let field = colon.map_or(self.line.as_slice(), |end| &self.line[..end]);
            if field == b"data" {
                let value = colon.map_or(&[][..], |end| &self.line[end + 1..]);
                let value = value.strip_prefix(b" ").unwrap_or(value);
                self.data.extend_from_slice(value);
                self.data.push(b'\n');
                self.has_data = true;
            }
        }
        self.line.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), ProtocolError> {
        if self.closed {
            return Err(ProtocolError::DecoderClosed);
        }
        self.closed = true;
        if !self.prefix.is_empty() {
            return Err(ProtocolError::InvalidUtf8);
        }
        std::str::from_utf8(&self.line).map_err(|_| ProtocolError::InvalidUtf8)?;
        if self.has_data || (!self.line.is_empty() && !self.line.starts_with(b":")) {
            return Err(ProtocolError::IncompleteFrame);
        }
        self.line.clear();
        Ok(())
    }

    fn fail<T>(&mut self, error: ProtocolError) -> Result<T, ProtocolError> {
        self.closed = true;
        self.line.clear();
        self.data.clear();
        Err(error)
    }
}

#[cfg(test)]
mod handover_tests {
    use super::*;

    #[test]
    fn every_cut_preserves_bom_crlf_and_partial_data() {
        let wire = b"\xef\xbb\xbfdata: first\r\ndata: second\r\n\r\n: beat\r\ndata: third\n\n";
        for cut in 0..=wire.len() {
            let mut old = SseDecoder::new(1024);
            let mut frames = old.push(&wire[..cut]).unwrap();
            let image =
                serde_json::from_slice(&serde_json::to_vec(&old.export().unwrap()).unwrap())
                    .unwrap();
            let mut new = SseDecoder::import(image, 1024).unwrap();
            frames.extend(new.push(&wire[cut..]).unwrap());
            new.finish().unwrap();
            assert_eq!(
                frames,
                [b"first\nsecond".to_vec(), b"third".to_vec()],
                "cut {cut}"
            );
        }
    }
}
