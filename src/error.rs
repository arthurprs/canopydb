use crate::utils::EscapedBytes;
use std::io;

/// String like type that occupies the same space as one usize
type TinyStr = Box<String>;

#[allow(missing_docs)]
#[derive(Debug, Display, Error)]
#[display("{:?}", self)]
#[non_exhaustive]
pub enum Error {
    DatabaseAlreadyOpen(#[error(not(source))] TinyStr),
    TreeAlreadyOpen(#[error(not(source))] TinyStr),
    Validation(#[error(not(source))] TinyStr),
    FreeList(#[error(not(source))] TinyStr),
    Io(io::Error),
    FatalIo(io::Error),
    EnvironmentHalted,
    DatabaseHalted,
    WalHalted,
    TransactionAborted,
    EnvironmentLocked,
    WriteTransactionRequired,
    CantCompact,
    WriteConflict,
}

impl Error {
    pub(crate) fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(Box::new(msg.into()))
    }

    pub(crate) fn database_already_open(name: impl Into<String>) -> Self {
        Self::DatabaseAlreadyOpen(Box::new(name.into()))
    }

    pub(crate) fn tree_already_open(name: &[u8]) -> Self {
        let name = EscapedBytes(name).to_string();
        Self::TreeAlreadyOpen(Box::new(name))
    }

    pub(crate) fn io_other(msg: &str) -> Self {
        Self::Io(io::Error::new(io::ErrorKind::Other, msg))
    }

    pub(crate) fn internal_clone(&self) -> Self {
        match self {
            Self::DatabaseAlreadyOpen(arg0) => Self::DatabaseAlreadyOpen(arg0.clone()),
            Self::TreeAlreadyOpen(arg0) => Self::TreeAlreadyOpen(arg0.clone()),
            Self::Validation(arg0) => Self::Validation(arg0.clone()),
            Self::FreeList(arg0) => Self::FreeList(arg0.clone()),
            Self::Io(arg0) => Self::Io(io_map!(arg0, "{arg0}")),
            Self::FatalIo(arg0) => Self::FatalIo(io_map!(arg0, "{arg0}")),
            Self::EnvironmentHalted => Self::EnvironmentHalted,
            Self::DatabaseHalted => Self::DatabaseHalted,
            Self::WalHalted => Self::WalHalted,
            Self::TransactionAborted => Self::TransactionAborted,
            Self::EnvironmentLocked => Self::EnvironmentLocked,
            Self::WriteTransactionRequired => Self::WriteTransactionRequired,
            Self::CantCompact => Self::CantCompact,
            Self::WriteConflict => Self::WriteConflict,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<Error> for io::Error {
    fn from(value: Error) -> Self {
        let kind = match &value {
            Error::Io(i) | Error::FatalIo(i) => i.kind(),
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Io(io::Error::new(io::ErrorKind::InvalidData, value))
    }
}

macro_rules! error_validation {
    ($($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        crate::Error::Validation(msg.into())
    }}
}

macro_rules! io_invalid_data {
    ($($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        let io_error = ::std::io::Error::new(::std::io::ErrorKind::InvalidData, msg);
        crate::Error::Io(io_error)
    }}
}

macro_rules! io_invalid_input {
    ($($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        let io_error = ::std::io::Error::new(::std::io::ErrorKind::InvalidInput, msg);
        crate::Error::Io(io_error)
    }}
}

macro_rules! io_other {
    ($($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        let io_error = ::std::io::Error::new(::std::io::ErrorKind::Other, msg);
        crate::Error::Io(io_error)
    }}
}

macro_rules! io_map {
    ($e: expr, $($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        ::std::io::Error::new($e.kind(), msg)
    }}
}

pub(crate) use error_validation;
pub(crate) use io_invalid_data;
pub(crate) use io_invalid_input;
pub(crate) use io_map;
pub(crate) use io_other;
