//! Who dials, and who sends, are two different questions.
//!
//! iOS can join a Wi-Fi network but never create one, so **Android always hosts
//! the network** (PRD §4.1) and therefore always owns the known address. The QR
//! carries Android's IP; iOS connects to it. Android cannot dial iOS, because
//! until iOS connects, Android does not know its DHCP address.
//!
//! That fixes the transport roles — **iOS is always the TCP client, Android
//! always the TCP server** — but not the data direction. Sending photos *from*
//! Android to an iPhone, which is the common case, needs the TCP server to be
//! the one transmitting.
//!
//! So this trait separates the two. A [`StreamSource`] yields established,
//! authenticated streams; whether it does that by dialling or by accepting is
//! its own business, and the sender and receiver logic works over either.

use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::tls::{self, Fingerprint, HostIdentity};
use crate::{Config, Error};

/// Supplies the streams of one session, in order: control first, then one per
/// data stream. Both ends call it the same number of times in the same order,
/// so a dialer on one side pairs with an acceptor on the other.
pub trait StreamSource: Send {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    fn next_stream(&mut self) -> impl Future<Output = Result<Self::Stream, Error>> + Send;
}

/// Dials the network host. Used by whichever device joined the Wi-Fi group —
/// in practice always iOS.
pub struct Dialer {
    connector: TlsConnector,
    server_name: rustls::pki_types::ServerName<'static>,
    addr: String,
    config: Config,
}

impl Dialer {
    pub fn new(
        addr: impl Into<String>,
        pinned: Fingerprint,
        config: &Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            connector: TlsConnector::from(tls::client_config(pinned)?),
            server_name: rustls::pki_types::ServerName::try_from(tls::SERVER_NAME)
                .map_err(|e| Error::Tls(format!("invalid server name: {e}")))?,
            addr: addr.into(),
            config: *config,
        })
    }
}

impl StreamSource for Dialer {
    type Stream = tokio_rustls::client::TlsStream<TcpStream>;

    async fn next_stream(&mut self) -> Result<Self::Stream, Error> {
        // Resolve first: the socket must be created for the right family, and
        // both interface pinning and buffer sizing have to happen *before*
        // connect — `SO_RCVBUF` only influences window scaling if it is set
        // before the handshake.
        let target = tokio::net::lookup_host(&self.addr)
            .await
            .map_err(|e| Error::Io(format!("resolving {}: {e}", self.addr)))?
            .next()
            .ok_or_else(|| Error::Io(format!("{} resolved to no address", self.addr)))?;

        let socket = if target.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|e| Error::Io(format!("creating socket: {e}")))?;

        crate::platform::bind_to_interface(&socket, self.config.bound_interface_index)?;
        report_buffers(crate::platform::set_socket_buffers(
            &socket,
            self.config.socket_send_buffer_bytes,
            self.config.socket_recv_buffer_bytes,
        ));

        let tcp = socket
            .connect(target)
            .await
            .map_err(|e| Error::Io(format!("connecting to {}: {e}", self.addr)))?;
        // Bulk transfer: never let Nagle hold a partial frame.
        tcp.set_nodelay(true).ok();

        self.connector
            .connect(self.server_name.clone(), tcp)
            .await
            .map_err(|e| Error::Tls(format!("handshake with {}: {e}", self.addr)))
    }
}

/// Accepts from the joining device. Used by the network host — in practice
/// always Android, whichever way the data flows.
pub struct Acceptor<'a> {
    listener: &'a TcpListener,
    acceptor: TlsAcceptor,
}

impl<'a> Acceptor<'a> {
    pub fn new(
        listener: &'a TcpListener,
        identity: &HostIdentity,
        config: &Config,
    ) -> Result<Self, Error> {
        // On the listener rather than each accepted socket: `SO_RCVBUF` only
        // influences window scaling if it is in place before the handshake, and
        // accepted sockets inherit it.
        report_buffers(crate::platform::set_socket_buffers(
            listener,
            config.socket_send_buffer_bytes,
            config.socket_recv_buffer_bytes,
        ));
        Ok(Self {
            listener,
            acceptor: TlsAcceptor::from(identity.server_config()?),
        })
    }
}

impl StreamSource for Acceptor<'_> {
    type Stream = tokio_rustls::server::TlsStream<TcpStream>;

    async fn next_stream(&mut self) -> Result<Self::Stream, Error> {
        let (tcp, _peer) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::Io(format!("accepting connection: {e}")))?;
        tcp.set_nodelay(true).ok();
        self.acceptor
            .accept(tcp)
            .await
            .map_err(|e| Error::Tls(format!("handshake: {e}")))
    }
}

/// Buffer sizing is best-effort — the kernel clamps to its own maximum — so a
/// failure is logged rather than raised.
fn report_buffers(result: crate::platform::BufferResult) {
    if let Some(Err(e)) = &result.send {
        tracing::warn!("could not set send buffer: {e}");
    }
    if let Some(Err(e)) = &result.recv {
        tracing::warn!("could not set receive buffer: {e}");
    }
}
