//! Local-only named-pipe boundary with exact logon-SID admission and client impersonation.

use std::{fs, io::Read as _, os::windows::io::OwnedHandle, path::Path, sync::Arc};

use interprocess::{
    local_socket::{
        GenericNamespaced, Listener as SyncListener, ListenerOptions, Stream as SyncStream,
        prelude::*, tokio::Stream as TokioStream, traits::tokio::Stream as _,
    },
    os::windows::{
        local_socket::ListenerOptionsExt as _, named_pipe::local_socket::tokio as tokio_pipe,
        security_descriptor::SecurityDescriptor,
    },
};
use sha2::{Digest as _, Sha256};
use widestring::U16CString;
use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};

use crate::service::InstalledServiceError;

pub(super) type Stream = TokioStream;

pub(super) struct Listener {
    inner: Arc<SyncListener>,
    logon_sid: Arc<SecurityIdentifier>,
}

impl Listener {
    pub(super) fn bind(root: &Path) -> Result<Self, InstalledServiceError> {
        fs::create_dir_all(root)?;
        let logon_sid = SecurityIdentifier::get_current_logon_sid()
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
            .ok_or(InstalledServiceError::BootstrapUnavailable)?;
        let sddl = U16CString::from_str(format!("D:P(A;;GA;;;{logon_sid})"))
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let descriptor = SecurityDescriptor::deserialize(&sddl)
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let name_text = pipe_name(root);
        let name = name_text.to_ns_name::<GenericNamespaced>()?;
        let inner = ListenerOptions::new()
            .name(name)
            .reclaim_name(false)
            .try_overwrite(false)
            .security_descriptor(descriptor)
            .create_sync()?;
        Ok(Self {
            inner: Arc::new(inner),
            logon_sid: Arc::new(logon_sid),
        })
    }

    pub(super) async fn accept(&self) -> Result<Stream, InstalledServiceError> {
        let listener = Arc::clone(&self.inner);
        let logon_sid = Arc::clone(&self.logon_sid);
        tokio::task::spawn_blocking(move || authenticate_and_handoff(&listener, &logon_sid))
            .await
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
    }
}

pub(super) async fn connect(root: &Path) -> Result<Stream, InstalledServiceError> {
    let name_text = pipe_name(root);
    let name = name_text.to_ns_name::<GenericNamespaced>()?;
    TokioStream::connect(name).await.map_err(Into::into)
}

pub(super) async fn authenticate_preface(
    _stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    // The synchronous authentication worker consumed the fixed preface before impersonation.
    Ok(())
}

fn authenticate_and_handoff(
    listener: &SyncListener,
    logon_sid: &SecurityIdentifier,
) -> Result<Stream, InstalledServiceError> {
    let mut stream = listener.accept()?;
    let SyncStream::NamedPipe(ref mut pipe) = stream;
    let mut preface = [0_u8; super::PREFACE.len()];
    pipe.read_exact(&mut preface)?;
    if preface != *super::PREFACE {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    {
        let _impersonation = pipe.inner().impersonate_client()?;
        if !SecurityIdentifier::is_current_user_member_of(logon_sid.as_sid())
            .map_err(|_error| InstalledServiceError::BootstrapRejected)?
        {
            return Err(InstalledServiceError::BootstrapRejected);
        }
    }
    let SyncStream::NamedPipe(pipe) = stream;
    let handle = OwnedHandle::from(pipe);
    let pipe = tokio_pipe::Stream::try_from(handle)
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    Ok(TokioStream::NamedPipe(pipe))
}

fn pipe_name(root: &Path) -> String {
    let digest = Sha256::digest(root.as_os_str().as_encoded_bytes());
    let mut name = String::from("market-squawk-bootstrap-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &digest[..16] {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    name
}
