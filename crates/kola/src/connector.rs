use crate::errors::KolaError;
use crate::serde6::{compress, decompress, deserialize, serialize};
use crate::serde9;
use crate::types::{MsgType, J};
use native_tls::TlsConnector;
use std::error::Error;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;
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
        let is_local = if host == "127.0.0.1" || host == "localhost" {
            true
        } else {
            false
        };
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

    pub fn send(&mut self, msg_type: MsgType, expr: &str, args: &Vec<J>) -> Result<(), KolaError> {
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let expr = expr.trim();
                // serde::serialize(stream, args.get_item(0).unwrap())
                if args.len() == 0 {
                    let length = 8 + 6 + expr.len();
                    let mut vec: Vec<u8> = Vec::with_capacity(length);
                    vec.write(&[1, msg_type as u8, 0, 0])?;
                    vec.write(&(length as u32).to_le_bytes())?;
                    vec.write(&[10, 0])?;
                    vec.write(&(expr.len() as u32).to_le_bytes())?;
                    vec.write(expr.as_bytes())?;
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
                    let mut vectors: Vec<Vec<u8>> = Vec::with_capacity(args.len());
                    for k in args.into_iter() {
                        vectors.push(serialize(k)?)
                    }
                    let is_lambda = expr.starts_with("{") && expr.ends_with("}");
                    let length = if is_lambda {
                        // 100 and 0
                        8 + 6 + 6 + 2 + expr.len()
                    } else {
                        8 + 6 + 6 + expr.len()
                    };
                    let mut total_length = length;
                    for v in vectors.iter() {
                        total_length += v.len();
                    }
                    let mut vec: Vec<u8> = Vec::with_capacity(length);
                    let length_ext = (total_length >> 32) as u8;
                    vec.write(&[1, msg_type as u8, 0, length_ext])?;
                    vec.write(&(total_length as u32).to_le_bytes())?;
                    vec.write(&[0, 0])?;
                    vec.write(&((args.len() + 1) as u32).to_le_bytes())?;
                    if is_lambda {
                        vec.write(&[100, 0])?;
                    }
                    vec.write(&[10, 0])?;
                    vec.write(&(expr.len() as u32).to_le_bytes())?;
                    vec.write(expr.as_bytes())?;
                    if self.is_local || total_length < 10_000_000 {
                        match stream.write_all(&vec) {
                            Ok(_) => (),
                            Err(e) => {
                                self.shutdown()?;
                                return Err(KolaError::IOError(e));
                            }
                        };
                        for vector in vectors.into_iter() {
                            match stream.write_all(&vector) {
                                Ok(_) => (),
                                Err(e) => {
                                    self.shutdown()?;
                                    return Err(KolaError::IOError(e));
                                }
                            }
                        }
                    } else {
                        let mut orignal_vec = Vec::with_capacity(total_length);
                        orignal_vec.write_all(&vec)?;
                        vectors.into_iter().for_each(|v| {
                            let _ = orignal_vec.write_all(&v);
                        });
                        stream.write_all(&compress(orignal_vec))?
                    };
                    Ok(())
                }
            } else {
                Err(KolaError::NotConnectedErr())
            }
        } else {
            if let Some(stream) = &mut self.stream {
                let expr = expr.trim();
                // serde::serialize(stream, args.get_item(0).unwrap())
                if args.len() == 0 {
                    let vec: Vec<u8> =
                        serde9::serialize(&J::String(expr.to_string()), !self.is_local)?;
                    stream.write(&[1, msg_type as u8, 0, 0, 0, 0, 0, 0])?;
                    stream.write(&vec.len().to_le_bytes())?;
                    match stream.write_all(&vec) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            self.shutdown()?;
                            Err(KolaError::IOError(e))
                        }
                    }
                } else {
                    let mut vectors: Vec<Vec<u8>> = Vec::with_capacity(args.len() + 1);
                    vectors.push(serde9::serialize(
                        &J::String(expr.to_string()),
                        !self.is_local,
                    )?);
                    for k in args.into_iter() {
                        vectors.push(serde9::serialize(k, !self.is_local)?)
                    }
                    let total_length = vectors.iter().map(|v| v.len()).sum::<usize>();
                    // 8 bytes header
                    stream.write(&[1, msg_type as u8, 0, 0, 0, 0, 0, 0])?;
                    // 8 bytes total length
                    stream.write(&(16 + total_length).to_le_bytes())?;
                    stream.write(&[90, 0, 0, 0])?;
                    stream.write(&(vectors.len() as u32).to_le_bytes())?;
                    stream.write(&total_length.to_le_bytes())?;
                    for vector in vectors.into_iter() {
                        match stream.write_all(&vector) {
                            Ok(_) => (),
                            Err(e) => {
                                self.shutdown()?;
                                return Err(KolaError::IOError(e));
                            }
                        }
                    }
                    Ok(())
                }
            } else {
                Err(KolaError::NotConnectedErr())
            }
        }
    }

    pub fn receive(&mut self) -> Result<J, KolaError> {
        if self.version <= 6 {
            if let Some(stream) = &mut self.stream {
                let mut header = [0u8; 8];
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
                let mut length = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
                length = length + ((header[3] as usize) << 32);
                let mut vec: Vec<u8> = vec![0u8; length - 8];
                match stream.read_exact(&mut vec) {
                    Ok(_) => (),
                    Err(e) => {
                        self.shutdown()?;
                        return Err(KolaError::IOError(e));
                    }
                };
                if compression_mode == 1 {
                    length = u32::from_le_bytes(vec[..4].try_into().unwrap()) as usize;
                    let mut de_vec = vec![0u8; length - 8];
                    decompress(&vec, &mut de_vec, 4);
                    deserialize(&de_vec, &mut 0, false)
                } else if compression_mode == 2 {
                    length = u64::from_le_bytes(vec[..8].try_into().unwrap()) as usize;
                    let mut de_vec = vec![0u8; length - 8];
                    decompress(&vec, &mut de_vec, 8);
                    deserialize(&de_vec, &mut 0, false)
                } else {
                    deserialize(&vec, &mut 0, false)
                }
            } else {
                Err(KolaError::NotConnectedErr())
            }
        } else {
            if let Some(stream) = &mut self.stream {
                let mut header = [0u8; 16];
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
                let length = u64::from_le_bytes(header[8..].try_into().unwrap()) as usize;
                let mut vec: Vec<u8> = vec![0u8; length];
                match stream.read_exact(&mut vec) {
                    Ok(_) => (),
                    Err(e) => {
                        self.shutdown()?;
                        return Err(KolaError::IOError(e));
                    }
                };
                serde9::deserialize(&vec, &mut 0)
            } else {
                Err(KolaError::NotConnectedErr())
            }
        }
    }

    pub fn connect(&mut self) -> Result<(), KolaError> {
        if let Some(_stream) = &self.stream {
            Ok(())
        } else {
            let socket = format!("{}:{}", &self.host, self.port);
            let mut tcp_stream = match TcpStream::connect(&socket) {
                Ok(stream) => stream,
                Err(e) => return Err(KolaError::IOError(e)),
            };
            tcp_stream.set_nodelay(true)?;
            if !self.timeout.is_zero() {
                tcp_stream
                    .set_read_timeout(Some(self.timeout))
                    .map_err(|e| KolaError::IOError(e))?;
            }

            if self.enable_tls {
                let connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true)
                    .build()
                    .map_err(|e| KolaError::Err(e.to_string()))?;
                let mut tls_stream = match connector.connect(&socket, tcp_stream) {
                    Ok(stream) => stream,
                    Err(e) => {
                        if let Some(e) = e.source() {
                            return Err(KolaError::FailedToConnectErr(e.to_string()));
                        }
                        return Err(KolaError::NotConnectedErr());
                    }
                };
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

    pub fn execute(&mut self, expr: &str, args: &Vec<J>) -> Result<J, KolaError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Sync, expr, args)?;
        self.receive()
    }

    pub fn execute_async(&mut self, expr: &str, args: &Vec<J>) -> Result<(), KolaError> {
        if self.stream.is_none() {
            self.connect()?;
        };
        self.send(MsgType::Async, expr, args)
    }
}
