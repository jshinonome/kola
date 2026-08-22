use std::net::IpAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender, TrySendError},
    Arc,
};
use std::thread::{self, JoinHandle};

use kola::connector::Connector;
use kola::errors::KolaError;
use napi::bindgen_prelude::{Env, Object};
use napi::{Error, JsDeferred, Status};
use napi_derive::napi;
use rustls_pki_types::ServerName;

use crate::dto::{
    k_into_native, native_values_into_k, snapshot_native_values, NativeOptions, NativeResult,
    NativeValue, OwnedNativeValue,
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
            Self::DisconnectProbe(reply) => {
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

#[napi(js_name = "NativeConnector")]
pub struct NativeConnector {
    sender: SyncSender<Command>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

#[napi]
impl NativeConnector {
    #[napi(constructor)]
    pub fn new(options: NativeOptions) -> napi::Result<Self> {
        let options = WorkerOptions::try_from(options)?;
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = thread::Builder::new()
            .name("kola-native-connector".into())
            .spawn(move || run_worker(receiver, options, worker_stopping))
            .map_err(|error| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to start native connector worker: {error}"),
                )
            })?;
        Ok(Self {
            sender,
            stopping,
            worker: Some(worker),
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

    #[napi(js_name = "sync", ts_return_type = "Promise<NativeResult>")]
    pub fn sync<'env>(
        &self,
        env: &'env Env,
        expression: String,
        args: Vec<NativeValue>,
    ) -> napi::Result<Object<'env>> {
        if let Err(error) = validate_expression_length(expression.len()) {
            return ready_result(env, NativeResult::failure(error));
        }
        match snapshot_native_values(args) {
            Ok(args) => self.enqueue(env, |reply| Command::Sync {
                expression,
                args,
                reply,
            }),
            Err(error) => ready_result(env, NativeResult::failure(error)),
        }
    }

    #[napi(js_name = "asyn", ts_return_type = "Promise<NativeResult>")]
    pub fn asyn<'env>(
        &self,
        env: &'env Env,
        expression: String,
        args: Vec<NativeValue>,
    ) -> napi::Result<Object<'env>> {
        if let Err(error) = validate_expression_length(expression.len()) {
            return ready_result(env, NativeResult::failure(error));
        }
        match snapshot_native_values(args) {
            Ok(args) => self.enqueue(env, |reply| Command::Asyn {
                expression,
                args,
                reply,
            }),
            Err(error) => ready_result(env, NativeResult::failure(error)),
        }
    }

    #[napi(js_name = "receive", ts_return_type = "Promise<NativeResult>")]
    pub fn receive<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        self.enqueue(env, Command::Receive)
    }
}

impl NativeConnector {
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

impl Drop for NativeConnector {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        let _ = self.sender.try_send(Command::Stop);
        drop(self.worker.take());
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

fn ready_result<'env>(env: &'env Env, result: NativeResult) -> napi::Result<Object<'env>> {
    let (deferred, promise) = env.create_deferred::<NativeResult, NativeResolver>()?;
    DeferredReply::new(deferred).resolve(result);
    Ok(promise)
}

fn run_worker(receiver: Receiver<Command>, options: WorkerOptions, stopping: Arc<AtomicBool>) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        run_worker_loop(&receiver, options, &stopping);
    }));
    stopping.store(true, Ordering::Release);
    let message = if outcome.is_err() {
        "native connector worker panicked"
    } else {
        "native connector worker stopped before replying"
    };
    fail_pending(&receiver, message);
}

fn run_worker_loop(receiver: &Receiver<Command>, options: WorkerOptions, stopping: &AtomicBool) {
    let mut connector = Connector::new(
        &options.host,
        options.port,
        &options.user,
        &options.password,
        options.tls,
        options.timeout_seconds,
        IPC_VERSION,
    );

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

        match command {
            Command::Connect(reply) => {
                reply.resolve(protect(|| {
                    NativeResult::from_result(
                        connector
                            .connect()
                            .map(|_| None)
                            .map_err(BindingError::from),
                    )
                }));
            }
            Command::Disconnect(reply) => {
                reply.resolve(protect(|| {
                    let result = match connector.shutdown() {
                        Ok(()) | Err(KolaError::NotConnectedErr()) => Ok(None),
                        Err(error) => Err(BindingError::from(error)),
                    };
                    NativeResult::from_result(result)
                }));
            }
            #[cfg(test)]
            Command::DisconnectProbe(reply) => {
                let result = protect(|| {
                    let result = match connector.shutdown() {
                        Ok(()) | Err(KolaError::NotConnectedErr()) => Ok(None),
                        Err(error) => Err(BindingError::from(error)),
                    };
                    NativeResult::from_result(result)
                });
                let _ = reply.send(result);
            }
            Command::Sync {
                expression,
                args,
                reply,
            } => {
                reply.resolve(protect(|| {
                    let result = native_values_into_k(args)
                        .and_then(|args| {
                            connector
                                .execute(&expression, &args)
                                .map_err(BindingError::from)
                        })
                        .and_then(k_into_native)
                        .map(Some);
                    NativeResult::from_result(result)
                }));
            }
            Command::Asyn {
                expression,
                args,
                reply,
            } => {
                reply.resolve(protect(|| {
                    let result = native_values_into_k(args).and_then(|args| {
                        connector
                            .execute_async(&expression, &args)
                            .map(|_| None)
                            .map_err(BindingError::from)
                    });
                    NativeResult::from_result(result)
                }));
            }
            Command::Receive(reply) => {
                reply.resolve(protect(|| {
                    let result = connector
                        .receive()
                        .map_err(BindingError::from)
                        .and_then(k_into_native)
                        .map(Some);
                    NativeResult::from_result(result)
                }));
            }
            Command::Stop => break,
            #[cfg(test)]
            Command::Probe {
                sequence,
                observed,
                reply,
            } => {
                let result = match observed.lock() {
                    Ok(mut observed) => {
                        observed.push(sequence);
                        NativeResult::success(None)
                    }
                    Err(_) => NativeResult::failure(BindingError::internal(
                        "probe observation lock was poisoned",
                    )),
                };
                let _ = reply.send(result);
            }
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

fn protect(operation: impl FnOnce() -> NativeResult) -> NativeResult {
    catch_unwind(AssertUnwindSafe(operation)).unwrap_or_else(|_| {
        NativeResult::failure(BindingError::internal("native operation panicked"))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::AtomicBool,
        mpsc::{self, TrySendError},
        Arc, Mutex,
    };
    use std::time::Duration;

    use napi::bindgen_prelude::{Env, Object};
    use napi::Status;

    use super::{
        admission_failure, run_worker, validate_expression_length, Command, NativeConnector,
        WorkerOptions, COMMAND_QUEUE_CAPACITY, MAX_EXPRESSION_BYTES, MAX_TIMEOUT_SECONDS,
    };
    use crate::dto::{NativeOptions, NativeValue};
    use crate::error::{CODE_BACKPRESSURE, CODE_CONVERSION, CODE_INTERNAL};

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
        let worker = std::thread::spawn(move || run_worker(receiver, options(), worker_stopping));
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
        let worker = std::thread::spawn(move || run_worker(receiver, options(), worker_stopping));

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
        let worker = std::thread::spawn(move || run_worker(receiver, options(), worker_stopping));

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
            String,
            Vec<NativeValue>,
        ) -> napi::Result<Object<'env>>;

        let _: [NoArgsMethod; 3] = [
            NativeConnector::connect,
            NativeConnector::disconnect,
            NativeConnector::receive,
        ];
        let _: [QueryMethod; 2] = [NativeConnector::sync, NativeConnector::asyn];
    }
}
