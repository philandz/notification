// tonic::Status is large by design; allow result_large_err across the crate
#![allow(clippy::result_large_err)]
// The double_must_use lint is raised by tonic/prost-generated code (include_proto!),
// which we cannot edit; allow it at crate level.
#![allow(clippy::double_must_use)]

pub mod pb {
    pub mod service {
        pub mod notification {
            tonic::include_proto!("service.notification");
        }
    }
}

pub mod config;
pub mod converters;
pub mod handler;
pub mod manager;
