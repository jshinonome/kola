use crate::errors::KolaError;
use crate::serde6::{compress, decompress, deserialize, serialize};
use crate::types::{MsgType, K};
use rustls::pki_types::ServerName;
use rustls::StreamOwned;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub(crate) trait QStream: IoRead + IoWrite {
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl<S: IoRead + IoWrite> QStream for S {
    fn shutdown(&self, _how: Shutdown) -> io::Result<()> {
        Ok(())
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
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(length).map_err(|error| {
        KolaError::Err(format!(
            "Unable to allocate {description} of {length} bytes: {error}"
        ))
    })?;
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
            is_local,
            timeout: Duration::new(timeout, 0),
            version,
        }
    }

    fn auth(&self, q_stream: &mut impl QStream) -> Result<(), KolaError> {
        let mut credential = format!("{}:{}", &self.user, &self.password)
            .as_bytes()
            .to_vec();
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
        if let Some(_stream) = &self.stream {
            Ok(())
        } else {
            let socket = format!("{}:{}", &self.host, self.port);
            let mut tcp_stream = if self.timeout.is_zero() {
                match TcpStream::connect(&socket) {
                    Ok(stream) => stream,
                    Err(e) => return Err(KolaError::IOError(e)),
                }
            } else {
                let addr = socket.to_socket_addrs().map_err(KolaError::IOError)?.next();
                if addr.is_none() {
                    return Err(KolaError::FailedToConnectErr(format!(
                        "Failed to connect to {}",
                        socket
                    )));
                }
                match TcpStream::connect_timeout(&addr.unwrap(), self.timeout) {
                    Ok(stream) => stream,
                    Err(e) => return Err(KolaError::IOError(e)),
                }
            };
            tcp_stream.set_nodelay(true)?;
            if !self.timeout.is_zero() {
                tcp_stream
                    .set_read_timeout(Some(self.timeout))
                    .map_err(KolaError::IOError)?;
                tcp_stream.set_write_timeout(Some(self.timeout))?;
            }

            if self.enable_tls {
                let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|e| KolaError::Err(e.to_string()))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
                .with_no_client_auth();
                let server_name = ServerName::try_from(self.host.as_str())
                    .unwrap_or_else(|_| {
                        let ip: std::net::IpAddr = self.host.parse().expect("invalid host");
                        ServerName::IpAddress(ip.into())
                    })
                    .to_owned();
                let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
                    .map_err(|e| KolaError::Err(e.to_string()))?;
                let mut tls_stream = StreamOwned::new(conn, tcp_stream);
                self.auth(&mut tls_stream)?;
                self.stream = Some(Box::new(tls_stream));
                Ok(())
            } else {
                self.auth(&mut tcp_stream)?;
                self.stream = Some(Box::new(tcp_stream));
                Ok(())
            }
        }
    }

    pub fn shutdown(&mut self) -> Result<(), KolaError> {
        if let Some(stream) = &self.stream {
            match stream.shutdown(Shutdown::Both) {
                Err(e) => {
                    self.stream = None;
                    Err(KolaError::IOError(e))
                }
                _ => {
                    self.stream = None;
                    Ok(())
                }
            }
        } else {
            Err(KolaError::NotConnectedErr())
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;
    use std::io::Cursor;

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
    fn tls_client_config_builds_without_panic() {
        let _config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("failed to set protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
        .with_no_client_auth();
    }

    #[test]
    fn no_cert_verifier_returns_supported_schemes() {
        let verifier = NoCertVerifier;
        let schemes = verifier.supported_verify_schemes();
        assert!(!schemes.is_empty(), "should return at least one scheme");
    }
}
