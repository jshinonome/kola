use crate::errors::KolaError;
use crate::serde6::{compress, decompress, deserialize, serialize};
use crate::types::{MsgType, K};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, StreamOwned};
use rustls_platform_verifier::BuilderVerifierExt;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) trait QStream: IoRead + IoWrite {}

impl<S: IoRead + IoWrite> QStream for S {}

#[derive(Debug)]
struct SharedTcpStream(Arc<TcpStream>);

impl IoRead for SharedTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.as_ref().read(buffer)
    }
}

impl IoWrite for SharedTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.as_ref().write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.as_ref().flush()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConnectorAbortHandle {
    active_stream: Arc<Mutex<Option<Arc<TcpStream>>>>,
}

impl ConnectorAbortHandle {
    pub fn abort(&self) -> Result<(), KolaError> {
        let stream = self
            .active_stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match stream {
            Some(stream) => match stream.shutdown(Shutdown::Both) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
                Err(error) => Err(KolaError::IOError(error)),
            },
            None => Ok(()),
        }
    }

    fn set_active_stream(&self, stream: Arc<TcpStream>) {
        *self
            .active_stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(stream);
    }
}

pub struct Connector {
    pub enable_tls: bool,
    pub is_local: bool,
    pub port: u16,
    pub version: u8,
    pub host: String,
    pub user: String,
    pub password: String,
    pub timeout: Duration,
    stream: Option<Box<dyn QStream + Send + Sync>>,
    abort_handle: ConnectorAbortHandle,
}

const IPC_HEADER_LENGTH: usize = 8;
const MIN_SERIALIZED_VALUE_LENGTH: usize = 2;
const MAX_IPC_MESSAGE_LENGTH: u64 = 512 * 1024 * 1024;
fn checked_outgoing_message_length(
    body_length: usize,
    description: &str,
) -> Result<(usize, u32), KolaError> {
    let total_length = body_length
        .checked_add(IPC_HEADER_LENGTH)
        .ok_or_else(|| KolaError::Err(format!("{description} length overflowed")))?;
    let total_length_u64 = u64::try_from(total_length).map_err(|_| {
        KolaError::Err(format!(
            "{description} length cannot be represented as a 64-bit IPC length"
        ))
    })?;
    if total_length_u64 > MAX_IPC_MESSAGE_LENGTH {
        return Err(KolaError::Err(format!(
            "{description} length {total_length_u64} exceeds the {MAX_IPC_MESSAGE_LENGTH}-byte safety limit"
        )));
    }
    let header_length = u32::try_from(total_length_u64).map_err(|_| {
        KolaError::Err(format!(
            "{description} length {total_length_u64} cannot be represented in the IPC header"
        ))
    })?;
    Ok((total_length, header_length))
}
fn allocate_buffer(length: usize, description: &str) -> Result<Vec<u8>, KolaError> {
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(length).map_err(|error| {
        KolaError::Err(format!(
            "Unable to allocate {description} of {length} bytes: {error}"
        ))
    })?;
    Ok(buffer)
}

fn checked_body_length(total_length: u64, description: &str) -> Result<usize, KolaError> {
    if total_length > MAX_IPC_MESSAGE_LENGTH {
        return Err(KolaError::Err(format!(
            "{description} length {total_length} exceeds the {MAX_IPC_MESSAGE_LENGTH}-byte safety limit"
        )));
    }

    let total_length = usize::try_from(total_length).map_err(|_| {
        KolaError::Err(format!(
            "{description} length cannot be represented on this platform"
        ))
    })?;
    let body_length = total_length.checked_sub(IPC_HEADER_LENGTH).ok_or_else(|| {
        KolaError::Err(format!(
            "{description} length {total_length} is shorter than the {IPC_HEADER_LENGTH}-byte header"
        ))
    })?;
    if body_length < MIN_SERIALIZED_VALUE_LENGTH {
        return Err(KolaError::Err(format!(
            "{description} body length {body_length} is too short to contain a serialized q value"
        )));
    }
    Ok(body_length)
}

fn allocate_zeroed_buffer(length: usize, description: &str) -> Result<Vec<u8>, KolaError> {
    let mut buffer = allocate_buffer(length, description)?;
    buffer.resize(length, 0);
    Ok(buffer)
}

fn compressed_message_length(body: &[u8], mode: u8) -> Result<(u64, usize), KolaError> {
    match mode {
        1 => {
            let prefix: [u8; 4] = body
                .get(..4)
                .ok_or_else(|| {
                    KolaError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 1 requires a 4-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    KolaError::Err("Invalid compression mode 1 length prefix".to_owned())
                })?;
            Ok((u64::from(u32::from_le_bytes(prefix)), 4))
        }
        2 => {
            let prefix: [u8; 8] = body
                .get(..8)
                .ok_or_else(|| {
                    KolaError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 2 requires an 8-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    KolaError::Err("Invalid compression mode 2 length prefix".to_owned())
                })?;
            Ok((u64::from_le_bytes(prefix), 8))
        }
        _ => Err(KolaError::Err(format!(
            "Unsupported IPC compression mode {mode}"
        ))),
    }
}

fn connect_to_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
    timeout: Duration,
) -> io::Result<TcpStream> {
    let started = Instant::now();
    let mut last_error = None;
    for address in addresses {
        let result = if timeout.is_zero() {
            TcpStream::connect(address)
        } else {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            };
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            }
            TcpStream::connect_timeout(&address, remaining)
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "host resolved to no socket addresses",
        )
    }))
}

fn tls_client_config() -> Result<ClientConfig, KolaError> {
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| KolaError::Err(error.to_string()))?
            .with_platform_verifier()
            .map_err(|error| KolaError::Err(error.to_string()))?;
    Ok(builder.with_no_client_auth())
}

fn tls_server_name(host: &str) -> Result<ServerName<'static>, KolaError> {
    ServerName::try_from(host.to_owned())
        .map_err(|error| KolaError::Err(format!("Invalid TLS server name: {error}")))
}

impl Connector {
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> Self {
        let host = if host.is_empty() { "127.0.0.1" } else { host };
        let is_local = host == "127.0.0.1" || host == "localhost";
        Connector {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            enable_tls,
            stream: None,
            abort_handle: ConnectorAbortHandle::default(),
            is_local,
            timeout: Duration::new(timeout, 0),
            version,
        }
    }

    pub fn abort_handle(&self) -> ConnectorAbortHandle {
        self.abort_handle.clone()
    }

    fn auth(&self, q_stream: &mut impl QStream) -> Result<(), KolaError> {
        let credential_length = self
            .user
            .len()
            .checked_add(self.password.len())
            .and_then(|length| length.checked_add(3))
            .ok_or_else(|| {
                KolaError::Err("Authentication credential length overflowed".to_owned())
            })?;
        let mut credential = allocate_buffer(credential_length, "authentication credential")?;
        credential.extend_from_slice(self.user.as_bytes());
        credential.push(b':');
        credential.extend_from_slice(self.password.as_bytes());
        credential.push(self.version);
        credential.push(0);
        q_stream.write_all(&credential)?;
        let mut support_version = [0u8];
        match q_stream.read(&mut support_version) {
            Ok(read_length) => {
                if read_length == 1 {
                    if support_version[0] >= 1 {
                        Ok(())
                    } else {
                        Err(KolaError::VersionErr())
                    }
                } else {
                    Err(KolaError::AuthErr())
                }
            }
            Err(e) => Err(KolaError::IOError(e)),
        }
    }

    pub fn send(&mut self, msg_type: MsgType, expr: &str, args: &[K]) -> Result<(), KolaError> {
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let expr = expr.trim();
                if args.is_empty() {
                    let body_length = 6usize.checked_add(expr.len()).ok_or_else(|| {
                        KolaError::Err("IPC request length overflowed".to_string())
                    })?;
                    let (total_length, header_length) =
                        checked_outgoing_message_length(body_length, "IPC request")?;
                    let expression_length =
                        i32::try_from(expr.len()).map_err(|_| KolaError::OverLengthErr())?;
                    let mut vec = allocate_buffer(total_length, "IPC request")?;
                    vec.write_all(&[1, msg_type as u8, 0, 0])?;
                    vec.write_all(&header_length.to_le_bytes())?;
                    vec.write_all(&[10, 0])?;
                    vec.write_all(&expression_length.to_le_bytes())?;
                    vec.write_all(expr.as_bytes())?;
                    match stream.write_all(&vec) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            self.shutdown()?;
                            Err(KolaError::IOError(e))
                        }
                    }
                } else {
                    if args.len() > 8 {
                        return Err(KolaError::TooManyArgumentErr());
                    }
                    let is_lambda = expr.starts_with('{') && expr.ends_with('}');
                    let body_prefix_length = 12usize
                        .checked_add(if is_lambda { 2 } else { 0 })
                        .and_then(|length| length.checked_add(expr.len()))
                        .ok_or_else(|| {
                            KolaError::Err("IPC request length overflowed".to_string())
                        })?;
                    let mut body_length = body_prefix_length;
                    for k in args {
                        body_length = body_length.checked_add(k.j6_len()?).ok_or_else(|| {
                            KolaError::Err("IPC request length overflowed".to_string())
                        })?;
                    }
                    let (total_length, header_length) =
                        checked_outgoing_message_length(body_length, "IPC request")?;
                    let expression_length =
                        i32::try_from(expr.len()).map_err(|_| KolaError::OverLengthErr())?;
                    let argument_count =
                        i32::try_from(args.len() + 1).map_err(|_| KolaError::OverLengthErr())?;

                    let mut vectors = Vec::new();
                    vectors.try_reserve_exact(args.len()).map_err(|error| {
                        KolaError::Err(format!(
                            "Unable to allocate serialized argument list: {error}"
                        ))
                    })?;
                    for k in args {
                        vectors.push(serialize(k)?);
                    }
                    let serialized_arguments_length =
                        vectors.iter().try_fold(0usize, |length, value| {
                            length.checked_add(value.len()).ok_or_else(|| {
                                KolaError::Err("Serialized argument length overflowed".to_string())
                            })
                        })?;
                    let prefix_length = IPC_HEADER_LENGTH
                        .checked_add(body_prefix_length)
                        .ok_or_else(|| {
                            KolaError::Err("IPC request length overflowed".to_string())
                        })?;
                    let actual_total_length = prefix_length
                        .checked_add(serialized_arguments_length)
                        .ok_or_else(|| {
                            KolaError::Err("IPC request length overflowed".to_string())
                        })?;
                    if actual_total_length != total_length {
                        return Err(KolaError::Err(
                            "Serialized argument length differs from its declared q length"
                                .to_string(),
                        ));
                    }

                    let mut vec = allocate_buffer(prefix_length, "IPC request prefix")?;
                    vec.write_all(&[1, msg_type as u8, 0, 0])?;
                    vec.write_all(&header_length.to_le_bytes())?;
                    vec.write_all(&[0, 0])?;
                    vec.write_all(&argument_count.to_le_bytes())?;
                    if is_lambda {
                        vec.write_all(&[100, 0])?;
                    }
                    vec.write_all(&[10, 0])?;
                    vec.write_all(&expression_length.to_le_bytes())?;
                    vec.write_all(expr.as_bytes())?;
                    if self.is_local || total_length < 10_000_000 {
                        match stream.write_all(&vec) {
                            Ok(_) => (),
                            Err(e) => {
                                self.shutdown()?;
                                return Err(KolaError::IOError(e));
                            }
                        };
                        for vector in vectors {
                            match stream.write_all(&vector) {
                                Ok(_) => (),
                                Err(e) => {
                                    self.shutdown()?;
                                    return Err(KolaError::IOError(e));
                                }
                            }
                        }
                    } else {
                        let mut original_vec =
                            allocate_buffer(total_length, "compressible IPC request")?;
                        original_vec.write_all(&vec)?;
                        for vector in vectors {
                            original_vec.write_all(&vector)?;
                        }
                        stream.write_all(&compress(original_vec))?
                    };
                    Ok(())
                }
            } else {
                Err(KolaError::NotConnectedErr())
            }
        } else {
            Err(KolaError::NotConnectedErr())
        }
    }

    pub fn receive(&mut self) -> Result<K, KolaError> {
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let mut header = [0u8; IPC_HEADER_LENGTH];
                match stream.read_exact(&mut header) {
                    Ok(_) => (),
                    Err(e) => {
                        self.shutdown()?;
                        return Err(KolaError::IOError(e));
                    }
                };
                let encoding = header[0];
                if encoding == 0 {
                    self.shutdown()?;
                    return Err(KolaError::NotSupportedBigEndianErr());
                }
                let compression_mode = header[2];
                if compression_mode > 2 {
                    self.shutdown()?;
                    return Err(KolaError::Err(format!(
                        "Unsupported IPC compression mode {compression_mode}"
                    )));
                }
                let low_length = u64::from(u32::from_le_bytes([
                    header[4], header[5], header[6], header[7],
                ]));
                let high_length = match u64::from(header[3]).checked_shl(32) {
                    Some(length) => length,
                    None => {
                        self.shutdown()?;
                        return Err(KolaError::Err(
                            "IPC message length extension overflowed".to_owned(),
                        ));
                    }
                };
                let total_length = match high_length.checked_add(low_length) {
                    Some(length) => length,
                    None => {
                        self.shutdown()?;
                        return Err(KolaError::Err("IPC message length overflowed".to_owned()));
                    }
                };
                let body_length = match checked_body_length(total_length, "IPC message") {
                    Ok(length) => length,
                    Err(error) => {
                        self.shutdown()?;
                        return Err(error);
                    }
                };
                let mut vec = match allocate_zeroed_buffer(body_length, "IPC message body") {
                    Ok(vec) => vec,
                    Err(error) => {
                        self.shutdown()?;
                        return Err(error);
                    }
                };
                match stream.read_exact(&mut vec) {
                    Ok(_) => (),
                    Err(e) => {
                        self.shutdown()?;
                        return Err(KolaError::IOError(e));
                    }
                };
                if compression_mode == 1 || compression_mode == 2 {
                    let (decompressed_length, prefix_length) =
                        match compressed_message_length(&vec, compression_mode) {
                            Ok(length) => length,
                            Err(error) => {
                                self.shutdown()?;
                                return Err(error);
                            }
                        };
                    let decompressed_body_length = match checked_body_length(
                        decompressed_length,
                        "Decompressed IPC message",
                    ) {
                        Ok(length) => length,
                        Err(error) => {
                            self.shutdown()?;
                            return Err(error);
                        }
                    };
                    let mut decompressed = match allocate_zeroed_buffer(
                        decompressed_body_length,
                        "decompressed IPC body",
                    ) {
                        Ok(vec) => vec,
                        Err(error) => {
                            self.shutdown()?;
                            return Err(error);
                        }
                    };
                    decompress(&vec, &mut decompressed, prefix_length)?;
                    deserialize(&decompressed, &mut 0, false)
                } else {
                    deserialize(&vec, &mut 0, false)
                }
            } else {
                Err(KolaError::NotConnectedErr())
            }
        } else {
            Err(KolaError::NotConnectedErr())
        }
    }

    pub fn connect(&mut self) -> Result<(), KolaError> {
        if self.stream.is_some() {
            return Ok(());
        }

        let tls = if self.enable_tls {
            Some((tls_server_name(&self.host)?, tls_client_config()?))
        } else {
            None
        };
        let mut addresses = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(KolaError::IOError)?
            .peekable();
        if addresses.peek().is_none() {
            return Err(KolaError::FailedToConnectErr(
                "host resolved to no socket addresses".to_owned(),
            ));
        }
        let tcp_stream =
            connect_to_addresses(addresses, self.timeout).map_err(KolaError::IOError)?;
        tcp_stream.set_nodelay(true)?;
        if !self.timeout.is_zero() {
            tcp_stream
                .set_read_timeout(Some(self.timeout))
                .map_err(KolaError::IOError)?;
            tcp_stream.set_write_timeout(Some(self.timeout))?;
        }

        let tcp_stream = Arc::new(tcp_stream);
        self.abort_handle.set_active_stream(Arc::clone(&tcp_stream));
        let shared_stream = SharedTcpStream(tcp_stream);
        let result = match tls {
            Some((server_name, config)) => {
                rustls::ClientConnection::new(Arc::new(config), server_name)
                    .map_err(|error| KolaError::Err(error.to_string()))
                    .and_then(|connection| {
                        self.install_authenticated_stream(StreamOwned::new(
                            connection,
                            shared_stream,
                        ))
                    })
            }
            None => self.install_authenticated_stream(shared_stream),
        };
        if result.is_err() {
            let _ = self.abort_handle.abort();
        }
        result
    }

    pub fn shutdown(&mut self) -> Result<(), KolaError> {
        if self.stream.take().is_none() {
            return Err(KolaError::NotConnectedErr());
        }
        self.abort_handle.abort()
    }

    fn install_authenticated_stream<S>(&mut self, mut stream: S) -> Result<(), KolaError>
    where
        S: QStream + Send + Sync + 'static,
    {
        self.auth(&mut stream)?;
        self.stream = Some(Box::new(stream));
        Ok(())
    }

    pub fn execute(&mut self, expr: &str, args: &[K]) -> Result<K, KolaError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Sync, expr, args)?;
        self.receive()
    }

    pub fn execute_async(&mut self, expr: &str, args: &[K]) -> Result<(), KolaError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Async, expr, args)
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        self.stream = None;
        let _ = self.abort_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn connector_with_response(response: Vec<u8>) -> Connector {
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector.stream = Some(Box::new(Cursor::new(response)));
        connector
    }

    fn response_header(compression_mode: u8, total_length: u64) -> Vec<u8> {
        let mut response = vec![
            1,
            2,
            compression_mode,
            (total_length >> 32) as u8,
            0,
            0,
            0,
            0,
        ];
        response[4..8].copy_from_slice(&(total_length as u32).to_le_bytes());
        response
    }

    #[test]
    fn receive_rejects_short_header() {
        let error = connector_with_response(vec![1, 2, 0])
            .receive()
            .expect_err("short IPC header should fail");
        assert!(matches!(error, KolaError::IOError(_)));
    }

    #[test]
    fn receive_rejects_length_shorter_than_header() {
        let mut connector = connector_with_response(response_header(0, 7));
        let error = connector
            .receive()
            .expect_err("invalid IPC length should fail");
        assert!(error.to_string().contains("shorter than the 8-byte header"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_unknown_compression_mode_and_disconnects() {
        let mut connector = connector_with_response(response_header(3, 10));
        let error = connector
            .receive()
            .expect_err("unknown compression mode should fail");
        assert!(error
            .to_string()
            .contains("Unsupported IPC compression mode 3"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_body_too_short_for_a_q_value() {
        for total_length in [8, 9] {
            let error = connector_with_response(response_header(0, total_length))
                .receive()
                .expect_err("empty or one-byte IPC body should fail");
            assert!(error
                .to_string()
                .contains("too short to contain a serialized q value"));
        }
    }

    #[test]
    fn receive_rejects_short_compression_prefixes() {
        for (compression_mode, prefix_length) in [(1, 4usize), (2, 8usize)] {
            let body = vec![0; prefix_length - 1];
            let error = compressed_message_length(&body, compression_mode)
                .expect_err("short compression prefix should fail");
            let message = error.to_string();
            assert!(
                message.contains("requires") && message.contains(&format!("{prefix_length}-byte")),
                "unexpected error: {message}"
            );
        }
    }

    #[test]
    fn receive_rejects_a_compressed_prefix_without_payload() {
        let mut response = response_header(1, 12);
        response.extend_from_slice(&10u32.to_le_bytes());
        assert!(matches!(
            connector_with_response(response).receive(),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn outgoing_frame_length_is_checked_before_header_conversion() {
        let maximum_body = usize::try_from(MAX_IPC_MESSAGE_LENGTH).expect("512 MiB fits usize")
            - IPC_HEADER_LENGTH;
        let (_, header_length) =
            checked_outgoing_message_length(maximum_body, "test frame").expect("limit-sized frame");
        assert_eq!(u64::from(header_length), MAX_IPC_MESSAGE_LENGTH);

        let error = checked_outgoing_message_length(maximum_body + 1, "test frame")
            .expect_err("oversized frame must fail");
        assert!(error.to_string().contains("safety limit"));
        assert!(checked_outgoing_message_length(usize::MAX, "test frame").is_err());
    }

    #[test]
    fn allocation_helpers_reserve_fallibly_and_zero_on_request() {
        let buffer = allocate_buffer(32, "test buffer").expect("allocation should succeed");
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= 32);
        assert_eq!(
            allocate_zeroed_buffer(4, "test buffer").expect("allocation should succeed"),
            vec![0; 4]
        );
        allocate_buffer(usize::MAX, "oversized test buffer")
            .expect_err("capacity overflow should be reported");
    }

    #[test]
    fn receive_rejects_trailing_frame_bytes() {
        let mut body = serialize(&K::I32(42)).expect("test value should serialize");
        body.push(0);
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut response = response_header(0, total_length);
        response.extend_from_slice(&body);
        let error = connector_with_response(response)
            .receive()
            .expect_err("trailing frame bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn receive_rejects_oversized_decompressed_length() {
        let mut response = response_header(2, 16);
        response.extend_from_slice(&u64::MAX.to_le_bytes());
        let error = connector_with_response(response)
            .receive()
            .expect_err("oversized decompressed length should fail");
        assert!(error.to_string().contains("Decompressed IPC"));
        assert!(error.to_string().contains("safety limit"));
    }
    #[test]
    fn receive_rejects_message_above_safety_limit() {
        let error = connector_with_response(response_header(0, MAX_IPC_MESSAGE_LENGTH + 1))
            .receive()
            .expect_err("oversized IPC message should fail");
        assert!(error.to_string().contains("exceeds"));
        assert!(error.to_string().contains("safety limit"));
    }

    #[test]
    fn tls_client_config_uses_platform_verification() {
        tls_client_config().expect("platform verifier configuration should build");
    }

    #[test]
    fn invalid_tls_server_name_returns_an_error_before_networking() {
        let mut connector = Connector::new("not a valid server name", 0, "", "", true, 0, 6);
        let error = connector
            .connect()
            .expect_err("invalid TLS server name should fail");
        assert!(matches!(
            error,
            KolaError::Err(message) if message.contains("Invalid TLS server name")
        ));
    }

    #[test]
    fn connection_attempts_each_resolved_address() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let available = listener
            .local_addr()
            .expect("listener should have an address");
        let unavailable = SocketAddr::from(([127, 0, 0, 1], 0));

        let stream = connect_to_addresses([unavailable, available], Duration::from_secs(1))
            .expect("second address should connect");
        let (accepted, _) = listener
            .accept()
            .expect("listener should accept connection");
        drop((stream, accepted));
    }

    #[test]
    fn abort_handle_interrupts_active_io_and_is_idempotent() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept connection");
            let mut credential = [0; 3];
            stream
                .read_exact(&mut credential)
                .expect("server should read authentication");
            stream
                .write_all(&[6])
                .expect("server should acknowledge authentication");
            let mut byte = [0];
            let _ = stream.read(&mut byte);
        });

        let mut connector = Connector::new("127.0.0.1", address.port(), "", "", false, 2, 6);
        let abort_handle = connector.abort_handle();
        connector.connect().expect("connector should authenticate");
        let worker = thread::spawn(move || connector.receive());

        let started = std::time::Instant::now();
        abort_handle.abort().expect("first abort should succeed");
        abort_handle
            .abort()
            .expect("repeated abort should be idempotent");
        let error = worker
            .join()
            .expect("connector worker should not panic")
            .expect_err("aborted receive should fail");
        assert!(matches!(error, KolaError::IOError(_)));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "abort should interrupt receive before the configured socket timeout"
        );
        server.join().expect("server should not panic");
    }
}
