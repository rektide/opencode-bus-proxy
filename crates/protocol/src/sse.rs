use crate::ProtocolError;

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
