//! Base types and re-exports for executor implementations.

use std::sync::Arc;

use futures::future::BoxFuture;

pub use apsched_core::error::ExecutorError;
pub use apsched_core::model::{JobOutcome, JobResultEnvelope, JobSpec};
pub use apsched_core::traits::Executor;

/// A handle to a callable that can be invoked by an executor.
///
/// In the Rust-only context, this is a boxed async function.
/// In the Python context, this wraps a PyObject (handled in the pyext crate).
#[derive(Clone)]
pub enum CallableHandle {
    /// A Rust async function (for testing and Rust-native jobs).
    RustFn(Arc<dyn Fn(JobSpec) -> BoxFuture<'static, JobOutcome> + Send + Sync>),
    /// Placeholder for Python callable — will be a PyObject in the pyext crate.
    /// Stored as a serialized callable reference for now.
    PythonRef(String),
}

impl std::fmt::Debug for CallableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallableHandle::RustFn(_) => write!(f, "CallableHandle::RustFn(...)"),
            CallableHandle::PythonRef(r) => write!(f, "CallableHandle::PythonRef({r})"),
        }
    }
}
