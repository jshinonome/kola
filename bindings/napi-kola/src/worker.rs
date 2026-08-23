use std::net::IpAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender, TrySendError},
    Arc,
};
use std::thread::{self, JoinHandle};

use kola::connector::{Connector, ConnectorAbortHandle};
use kola::errors::KolaError;
use napi::bindgen_prelude::{Array, Env, Object};
use napi::{Error, JsDeferred, JsString, Status};
use napi_derive::napi;
use rustls_pki_types::ServerName;

use crate::dto::{
    k_into_native, native_values_into_k, snapshot_native_values, NativeOptions, NativeResult,
    OwnedNativeValue,
};
use crate::error::BindingError;

const IPC_VERSION: u8 = 6;
const COMMAND_QUEUE_CAPACITY: usize = 8;
const MAX_TIMEOUT_SECONDS: u32 = 86_400;
const MAX_EXPRESSION_BYTES: usize = 64 * 1024 * 1024;

fn validate_expression_length(length: usize) -> Result<(), BindingError> {
    if length > MAX_EXPRESSION_BYTES {
        return Err(BindingError::conversion(format!(
            "q expression exceeds its {MAX_EXPRESSION_BYTES} byte limit"
        )));
    }
    Ok(())
}

type NativeResolver = Box<dyn FnOnce(Env) -> napi::Result<NativeResult> + Send + 'static>;
type NativeDeferred = JsDeferred<NativeResult, NativeResolver>;

struct WorkerOptions {
    host: String,
    port: u16,
    user: String,
    password: String,
    tls: bool,
    timeout_seconds: u64,
}

struct DeferredReply(Option<NativeDeferred>);

impl DeferredReply {
    fn new(deferred: NativeDeferred) -> Self {
        Self(Some(deferred))
    }

    fn resolve(mut self, result: NativeResult) {
        if let Some(deferred) = self.0.take() {
            deferred.resolve(Box::new(move |_env| Ok(result)));
        }
    }
}

impl Drop for DeferredReply {
    fn drop(&mut self) {
        if let Some(deferred) = self.0.take() {
            deferred.resolve(Box::new(move |_env| {
                Ok(NativeResult::failure(BindingError::internal(
                    "native connector worker stopped before replying",
                )))
            }));
        }
    }
}

enum Command {
    Connect(DeferredReply),
    Disconnect(DeferredReply),
    Sync {
        expression: String,
        args: Vec<OwnedNativeValue>,
        reply: DeferredReply,
    },
    Asyn {
        expression: String,
        args: Vec<OwnedNativeValue>,
        reply: DeferredReply,
    },
    Receive(DeferredReply),
    #[cfg(test)]
    DisconnectProbe(mpsc::Sender<NativeResult>),
    #[cfg(test)]
    ReceiveProbe {
        started: mpsc::Sender<()>,
        reply: mpsc::Sender<NativeResult>,
    },
    #[cfg(test)]
    PanicProbe(mpsc::Sender<NativeResult>),
    Stop,
    #[cfg(test)]
    Probe {
        sequence: usize,
        observed: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        reply: mpsc::Sender<NativeResult>,
    },
}

impl Command {
    fn fail(self, error: BindingError) {
        let result = NativeResult::failure(error);
        match self {
            Self::Connect(reply) | Self::Disconnect(reply) | Self::Receive(reply) => {
                reply.resolve(result);
            }
            Self::Sync { reply, .. } | Self::Asyn { reply, .. } => reply.resolve(result),
            #[cfg(test)]
            Self::DisconnectProbe(reply)
            | Self::PanicProbe(reply)
            | Self::ReceiveProbe { reply, .. } => {
                let _ = reply.send(result);
            }
            Self::Stop => {}
            #[cfg(test)]
            Self::Probe { reply, .. } => {
                let _ = reply.send(result);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ExecutionMode {
    Sync,
    Asyn,
}

#[napi(js_name = "NativeConnector")]
pub struct NativeConnector {
    sender: SyncSender<Command>,
    stopping: Arc<AtomicBool>,
    abort_handle: ConnectorAbortHandle,
    worker: Option<JoinHandle<()>>,
    reaper: mpsc::Sender<JoinHandle<()>>,
}

#[napi]
impl NativeConnector {
    #[napi(constructor)]
    pub fn new(options: NativeOptions) -> napi::Result<Self> {
        let connector = WorkerOptions::try_from(options)?.into_connector();
        let abort_handle = connector.abort_handle();
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = thread::Builder::new()
            .name("kola-native-connector".into())
            .spawn(move || run_worker(receiver, connector, worker_stopping))
            .map_err(|error| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to start native connector worker: {error}"),
                )
            })?;
        let reaper = match spawn_worker_reaper(None) {
            Ok(reaper) => reaper,
            Err(error) => {
                stopping.store(true, Ordering::Release);
                let _ = sender.try_send(Command::Stop);
                let _ = worker.join();
                return Err(Error::new(
                    Status::GenericFailure,
                    format!("failed to start native connector worker reaper: {error}"),
                ));
            }
        };
        Ok(Self {
            sender,
            stopping,
            abort_handle,
            worker: Some(worker),
            reaper,
        })
    }

    #[napi(js_name = "connect", ts_return_type = "Promise<NativeResult>")]
    pub fn connect<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.enqueue(env, Command::Connect)
    }

    #[napi(js_name = "disconnect", ts_return_type = "Promise<NativeResult>")]
    pub fn disconnect<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.enqueue(env, Command::Disconnect)
    }

    #[napi(
        js_name = "sync",
        ts_args_type = "expression: string, args: NativeValue[]",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn sync<'env>(
        &self,
        env: &'env Env,
        expression: JsString<'env>,
        args: Array<'env>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_execution(env, expression, args, ExecutionMode::Sync)
    }

    #[napi(
        js_name = "asyn",
        ts_args_type = "expression: string, args: NativeValue[]",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn asyn<'env>(
        &self,
        env: &'env Env,
        expression: JsString<'env>,
        args: Array<'env>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_execution(env, expression, args, ExecutionMode::Asyn)
    }

    #[napi(js_name = "receive", ts_return_type = "Promise<NativeResult>")]
    pub fn receive<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.enqueue(env, Command::Receive)
    }
}

impl NativeConnector {
    fn enqueue_execution<'env>(
        &self,
        env: &'env Env,
        expression: JsString<'env>,
        args: Array<'env>,
        mode: ExecutionMode,
    ) -> napi::Result<Object<'env>> {
        let expression_length = expression.utf8_len()?;
        if let Err(error) = validate_expression_length(expression_length) {
            return ready_result(env, NativeResult::failure(error));
        }
        let expression = expression.into_utf8()?.into_owned()?;
        match snapshot_native_values(args) {
            Ok(args) => self.enqueue(env, |reply| match mode {
                ExecutionMode::Sync => Command::Sync {
                    expression,
                    args,
                    reply,
                },
                ExecutionMode::Asyn => Command::Asyn {
                    expression,
                    args,
                    reply,
                },
            }),
            Err(error) => ready_result(env, NativeResult::failure(error)),
        }
    }
    fn enqueue<'env>(
        &self,
        env: &'env Env,
        command: impl FnOnce(DeferredReply) -> Command,
    ) -> napi::Result<Object<'env>> {
        let (deferred, promise) = env.create_deferred::<NativeResult, NativeResolver>()?;
        let command = command(DeferredReply::new(deferred));

        if self.stopping.load(Ordering::Acquire) {
            command.fail(BindingError::internal(
                "native connector worker is not running",
            ));
            return Ok(promise);
        }

        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(error) => {
                let failure = admission_failure(&error);
                let command = match error {
                    TrySendError::Full(command) | TrySendError::Disconnected(command) => command,
                };
                command.fail(failure);
            }
        }
        Ok(promise)
    }
}

fn admission_failure<T>(error: &TrySendError<T>) -> BindingError {
    match error {
        TrySendError::Full(_) => {
            BindingError::backpressure("native connector command queue is full")
        }
        TrySendError::Disconnected(_) => {
            BindingError::internal("native connector worker is not running")
        }
    }
}

fn spawn_worker_reaper(
    joined: Option<mpsc::Sender<()>>,
) -> std::io::Result<mpsc::Sender<JoinHandle<()>>> {
    let (sender, receiver) = mpsc::channel::<JoinHandle<()>>();
    thread::Builder::new()
        .name("kola-napi-worker-reaper".to_owned())
        .spawn(move || {
            if let Ok(worker) = receiver.recv() {
                let _ = worker.join();
                if let Some(joined) = joined {
                    let _ = joined.send(());
                }
            }
        })?;
    Ok(sender)
}

impl Drop for NativeConnector {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        let _ = self.abort_handle.abort();
        let _ = self.sender.try_send(Command::Stop);
        if let Some(worker) = self.worker.take() {
            self.reaper
                .send(worker)
                .expect("worker reaper must remain available until connector drop");
        }
    }
}

impl TryFrom<NativeOptions> for WorkerOptions {
    type Error = napi::Error;

    fn try_from(options: NativeOptions) -> Result<Self, Self::Error> {
        let host = if options.host.is_empty() {
            "127.0.0.1".to_owned()
        } else {
            options.host
        };
        let tls = options.tls.unwrap_or(false);
        if tls && host.parse::<IpAddr>().is_err() && ServerName::try_from(host.as_str()).is_err() {
            return Err(Error::new(
                Status::InvalidArg,
                "host must be a valid DNS name or IP address when tls is enabled",
            ));
        }
        let timeout_seconds = options.timeout_seconds.unwrap_or(30);
        if timeout_seconds == 0 {
            return Err(Error::new(
                Status::InvalidArg,
                "timeoutSeconds must be at least 1 so the native worker can terminate cleanly",
            ));
        }
        if timeout_seconds > MAX_TIMEOUT_SECONDS {
            return Err(Error::new(
                Status::InvalidArg,
                "timeoutSeconds must not exceed 86400 (24 hours)",
            ));
        }
        Ok(Self {
            host,
            port: options.port,
            user: options.user.unwrap_or_default(),
            password: options.password.unwrap_or_default(),
            tls,
            timeout_seconds: timeout_seconds as u64,
        })
    }
}

impl WorkerOptions {
    fn into_connector(self) -> Connector {
        Connector::new(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            self.tls,
            self.timeout_seconds,
            IPC_VERSION,
        )
    }
}

fn ready_result<'env>(env: &'env Env, result: NativeResult) -> napi::Result<Object<'env>> {
    let (deferred, promise) = env.create_deferred::<NativeResult, NativeResolver>()?;
    DeferredReply::new(deferred).resolve(result);
    Ok(promise)
}

fn run_worker(receiver: Receiver<Command>, connector: Connector, stopping: Arc<AtomicBool>) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        run_worker_loop(&receiver, connector, &stopping);
    }));
    stopping.store(true, Ordering::Release);
    let message = if outcome.is_err() {
        "native connector worker panicked"
    } else {
        "native connector worker stopped before replying"
    };
    fail_pending(&receiver, message);
}

fn run_worker_loop(receiver: &Receiver<Command>, mut connector: Connector, stopping: &AtomicBool) {
    while !stopping.load(Ordering::Acquire) {
        let command = match receiver.recv() {
            Ok(command) => command,
            Err(_) => break,
        };
        if stopping.load(Ordering::Acquire) {
            command.fail(BindingError::internal(
                "native connector worker is stopping",
            ));
            break;
        }

        let keep_running = match command {
            Command::Connect(reply) => complete_operation(
                |result| reply.resolve(result),
                || {
                    NativeResult::from_result(
                        connector
                            .connect()
                            .map(|_| None)
                            .map_err(BindingError::from),
                    )
                },
            ),
            Command::Disconnect(reply) => complete_operation(
                |result| reply.resolve(result),
                || {
                    let result = match connector.shutdown() {
                        Ok(()) | Err(KolaError::NotConnectedErr()) => Ok(None),
                        Err(error) => Err(BindingError::from(error)),
                    };
                    NativeResult::from_result(result)
                },
            ),
            #[cfg(test)]
            Command::DisconnectProbe(reply) => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || {
                    let result = match connector.shutdown() {
                        Ok(()) | Err(KolaError::NotConnectedErr()) => Ok(None),
                        Err(error) => Err(BindingError::from(error)),
                    };
                    NativeResult::from_result(result)
                },
            ),
            #[cfg(test)]
            Command::ReceiveProbe { started, reply } => {
                let _ = started.send(());
                complete_operation(
                    |result| {
                        let _ = reply.send(result);
                    },
                    || {
                        NativeResult::from_result(
                            connector
                                .receive()
                                .map_err(BindingError::from)
                                .and_then(k_into_native)
                                .map(Some),
                        )
                    },
                )
            }
            #[cfg(test)]
            Command::PanicProbe(reply) => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || panic!("worker panic probe"),
            ),
            Command::Sync {
                expression,
                args,
                reply,
            } => complete_operation(
                |result| reply.resolve(result),
                || {
                    let result = native_values_into_k(args)
                        .and_then(|args| {
                            connector
                                .execute(&expression, &args)
                                .map_err(BindingError::from)
                        })
                        .and_then(k_into_native)
                        .map(Some);
                    NativeResult::from_result(result)
                },
            ),
            Command::Asyn {
                expression,
                args,
                reply,
            } => complete_operation(
                |result| reply.resolve(result),
                || {
                    let result = native_values_into_k(args).and_then(|args| {
                        connector
                            .execute_async(&expression, &args)
                            .map(|_| None)
                            .map_err(BindingError::from)
                    });
                    NativeResult::from_result(result)
                },
            ),
            Command::Receive(reply) => complete_operation(
                |result| reply.resolve(result),
                || {
                    let result = connector
                        .receive()
                        .map_err(BindingError::from)
                        .and_then(k_into_native)
                        .map(Some);
                    NativeResult::from_result(result)
                },
            ),
            Command::Stop => false,
            #[cfg(test)]
            Command::Probe {
                sequence,
                observed,
                reply,
            } => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || match observed.lock() {
                    Ok(mut observed) => {
                        observed.push(sequence);
                        NativeResult::success(None)
                    }
                    Err(_) => NativeResult::failure(BindingError::internal(
                        "probe observation lock was poisoned",
                    )),
                },
            ),
        };
        if !keep_running {
            break;
        }
    }

    stopping.store(true, Ordering::Release);
    fail_pending(receiver, "native connector worker is stopping");
    let _ = connector.shutdown();
}

fn fail_pending(receiver: &Receiver<Command>, message: &'static str) {
    while let Ok(command) = receiver.try_recv() {
        command.fail(BindingError::internal(message));
    }
}

fn complete_operation(
    resolve: impl FnOnce(NativeResult),
    operation: impl FnOnce() -> NativeResult,
) -> bool {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => {
            resolve(result);
            true
        }
        Err(_) => {
            resolve(NativeResult::failure(BindingError::internal(
                "native operation panicked",
            )));
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, TrySendError},
        Arc, Mutex,
    };
    use std::time::{Duration, Instant};

    use napi::bindgen_prelude::{Array, Env, Object};
    use napi::{JsString, Status};

    use super::{
        admission_failure, run_worker, spawn_worker_reaper, validate_expression_length, Command,
        Connector, NativeConnector, WorkerOptions, COMMAND_QUEUE_CAPACITY, MAX_EXPRESSION_BYTES,
        MAX_TIMEOUT_SECONDS,
    };
    use crate::dto::NativeOptions;
    use crate::error::{CODE_BACKPRESSURE, CODE_CONVERSION, CODE_INTERNAL, CODE_IO};

    fn options() -> WorkerOptions {
        WorkerOptions {
            host: "127.0.0.1".into(),
            port: 1,
            user: String::new(),
            password: String::new(),
            tls: false,
            timeout_seconds: 0,
        }
    }

    fn connector() -> Connector {
        options().into_connector()
    }

    fn native_options(timeout_seconds: u32) -> NativeOptions {
        NativeOptions {
            host: "127.0.0.1".into(),
            port: 1,
            user: None,
            password: None,
            tls: None,
            timeout_seconds: Some(timeout_seconds),
        }
    }

    #[test]
    fn processes_admitted_commands_in_fifo_order() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut replies = Vec::new();

        for sequence in 0..64 {
            let (reply, response) = mpsc::channel();
            sender
                .send(Command::Probe {
                    sequence,
                    observed: Arc::clone(&observed),
                    reply,
                })
                .expect("admit probe");
            replies.push(response);
        }
        for reply in replies {
            assert!(
                reply
                    .recv_timeout(Duration::from_secs(1))
                    .expect("probe reply")
                    .ok
            );
        }
        sender.send(Command::Stop).expect("stop worker");
        worker.join().expect("join worker");

        assert_eq!(
            *observed.lock().expect("observed order"),
            (0..64).collect::<Vec<_>>()
        );
    }

    #[test]
    fn disconnect_is_idempotent_without_a_server() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));

        for _ in 0..2 {
            let (reply, response) = mpsc::channel();
            sender
                .send(Command::DisconnectProbe(reply))
                .expect("disconnect");
            assert!(
                response
                    .recv_timeout(Duration::from_secs(1))
                    .expect("disconnect reply")
                    .ok
            );
        }

        sender.send(Command::Stop).expect("stop worker");
        worker.join().expect("join worker");
    }

    #[test]
    fn stopping_rejects_queued_commands_without_executing_the_backlog() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut responses = Vec::new();
        for sequence in 0..4 {
            let (reply, response) = mpsc::channel();
            sender
                .try_send(Command::Probe {
                    sequence,
                    observed: Arc::clone(&observed),
                    reply,
                })
                .expect("queue probe");
            responses.push(response);
        }

        let stopping = Arc::new(AtomicBool::new(true));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));

        for response in responses {
            let result = response
                .recv_timeout(Duration::from_secs(1))
                .expect("teardown reply");
            assert!(!result.ok);
            assert_eq!(result.error.expect("teardown error").code, CODE_INTERNAL);
        }
        worker.join().expect("join worker");
        assert!(observed.lock().expect("observed backlog").is_empty());
    }

    #[test]
    fn command_panic_is_terminal_and_fails_queued_work() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let (panic_reply, panic_response) = mpsc::channel();
        sender
            .send(Command::PanicProbe(panic_reply))
            .expect("queue panic probe");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let (queued_reply, queued_response) = mpsc::channel();
        sender
            .send(Command::Probe {
                sequence: 1,
                observed: Arc::clone(&observed),
                reply: queued_reply,
            })
            .expect("queue work after panic");

        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || {
            run_worker(receiver, connector(), worker_stopping);
        });

        let panic_result = panic_response
            .recv_timeout(Duration::from_secs(1))
            .expect("panic reply");
        assert!(!panic_result.ok);
        assert_eq!(panic_result.error.expect("panic error").code, CODE_INTERNAL);
        let queued_result = queued_response
            .recv_timeout(Duration::from_secs(1))
            .expect("queued failure");
        assert!(!queued_result.ok);
        assert_eq!(
            queued_result.error.expect("queued error").code,
            CODE_INTERNAL
        );
        worker.join().expect("join worker");
        assert!(stopping.load(Ordering::Acquire));
        assert!(observed.lock().expect("observed work").is_empty());
    }

    #[test]
    fn drop_aborts_active_receive_and_joins_worker() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
        let port = listener.local_addr().expect("server address").port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept connector");
            loop {
                let mut byte = [0];
                stream.read_exact(&mut byte).expect("read credentials");
                if byte[0] == 0 {
                    break;
                }
            }
            stream.write_all(&[6]).expect("write IPC version");
            let mut byte = [0];
            let _ = stream.read(&mut byte);
        });

        let mut worker_options = options();
        worker_options.port = port;
        worker_options.timeout_seconds = 30;
        let mut connector = worker_options.into_connector();
        connector.connect().expect("connect test client");
        let abort_handle = connector.abort_handle();
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || {
            run_worker(receiver, connector, worker_stopping);
        });
        let (reaper_joined, joined) = mpsc::channel();
        let reaper = spawn_worker_reaper(Some(reaper_joined)).expect("start worker reaper");
        let (started, receive_started) = mpsc::channel();
        let (reply, response) = mpsc::channel();
        sender
            .send(Command::ReceiveProbe { started, reply })
            .expect("queue receive");
        receive_started
            .recv_timeout(Duration::from_secs(1))
            .expect("receive started");

        drop(NativeConnector {
            sender,
            stopping: Arc::clone(&stopping),
            abort_handle,
            worker: Some(worker),
            reaper,
        });

        assert!(stopping.load(Ordering::Acquire));
        joined
            .recv_timeout(Duration::from_secs(1))
            .expect("worker should be joined after abort");
        let result = response
            .recv_timeout(Duration::from_secs(1))
            .expect("aborted receive reply");
        assert!(!result.ok);
        assert_eq!(result.error.expect("receive error").code, CODE_IO);
        server.join().expect("join test server");
    }

    #[test]
    fn drop_does_not_wait_for_worker_without_an_active_socket() {
        let abort_handle = connector().abort_handle();
        let (sender, _receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let (release_worker, wait_for_release) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            wait_for_release.recv().expect("release worker");
        });
        let (reaper_joined, joined) = mpsc::channel();
        let reaper = spawn_worker_reaper(Some(reaper_joined)).expect("start worker reaper");

        let started = Instant::now();
        drop(NativeConnector {
            sender,
            stopping: Arc::clone(&stopping),
            abort_handle,
            worker: Some(worker),
            reaper,
        });

        assert!(stopping.load(Ordering::Acquire));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "drop must not wait for an uninterruptible connection attempt"
        );
        release_worker.send(()).expect("release worker");
        joined
            .recv_timeout(Duration::from_secs(1))
            .expect("reaper should join worker");
    }

    #[test]
    fn bounded_queue_rejects_immediately_when_full() {
        let (sender, _receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        for sequence in 0..COMMAND_QUEUE_CAPACITY {
            sender.try_send(sequence).expect("fill queue");
        }

        let error = sender
            .try_send(COMMAND_QUEUE_CAPACITY)
            .expect_err("full queue must reject");
        assert!(matches!(
            &error,
            TrySendError::Full(value) if *value == COMMAND_QUEUE_CAPACITY
        ));
        assert_eq!(admission_failure(&error).code, CODE_BACKPRESSURE);
    }

    #[test]
    fn disconnected_queue_reports_internal_failure() {
        let (sender, receiver) = mpsc::sync_channel::<usize>(COMMAND_QUEUE_CAPACITY);
        drop(receiver);

        let error = sender.try_send(0).expect_err("worker is disconnected");
        assert!(matches!(
            &error,
            TrySendError::Disconnected(value) if *value == 0
        ));
        assert_eq!(admission_failure(&error).code, CODE_INTERNAL);
    }

    #[test]
    fn expression_utf8_length_is_bounded_before_queue_admission() {
        assert!(validate_expression_length(MAX_EXPRESSION_BYTES).is_ok());
        let error = validate_expression_length(MAX_EXPRESSION_BYTES + 1)
            .expect_err("oversized expression must be rejected");
        assert_eq!(error.code, CODE_CONVERSION);
        assert!(error.message.contains("byte limit"));
    }
    #[test]
    fn timeout_seconds_is_bounded_to_twenty_four_hours() {
        let accepted = WorkerOptions::try_from(native_options(MAX_TIMEOUT_SECONDS))
            .unwrap_or_else(|error| panic!("maximum timeout should be accepted: {error}"));
        assert_eq!(accepted.timeout_seconds, u64::from(MAX_TIMEOUT_SECONDS));

        for invalid in [0, MAX_TIMEOUT_SECONDS + 1] {
            let error = WorkerOptions::try_from(native_options(invalid))
                .err()
                .expect("invalid timeout must be rejected");
            assert_eq!(error.status, Status::InvalidArg);
        }
    }

    #[test]
    fn native_methods_keep_promise_return_signatures() {
        type NoArgsMethod = for<'env> fn(&NativeConnector, &'env Env) -> napi::Result<Object<'env>>;
        type QueryMethod = for<'env> fn(
            &NativeConnector,
            &'env Env,
            JsString<'env>,
            Array<'env>,
        ) -> napi::Result<Object<'env>>;

        let _: [NoArgsMethod; 3] = [
            NativeConnector::connect,
            NativeConnector::disconnect,
            NativeConnector::receive,
        ];
        let _: [QueryMethod; 2] = [NativeConnector::sync, NativeConnector::asyn];
    }
}
