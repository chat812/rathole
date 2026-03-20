pub const HASH_WIDTH_IN_BYTES: usize = 32;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

use crate::config::ServiceType;

type ProtocolVersion = u8;
const _PROTO_V0: u8 = 0u8;
const _PROTO_V1: u8 = 1u8;
#[allow(dead_code)]
const PROTO_V2: u8 = 2u8;

// Use PROTO_V1 value for wire compat but with v2 framing for control cmds
pub const CURRENT_PROTO_VERSION: ProtocolVersion = _PROTO_V1;

pub type Digest = [u8; HASH_WIDTH_IN_BYTES];

#[derive(Deserialize, Serialize, Debug)]
pub enum Hello {
    ControlChannelHello(ProtocolVersion, Digest), // sha256sum(service name) or a nonce
    DataChannelHello(ProtocolVersion, Digest),    // token provided by CreateDataChannel
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Auth(pub Digest);

#[derive(Deserialize, Serialize, Debug)]
pub enum Ack {
    Ok,
    ServiceNotExist,
    AuthFailed,
}

impl std::fmt::Display for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Ack::Ok => "Ok",
                Ack::ServiceNotExist => "Service not exist",
                Ack::AuthFailed => "Incorrect token",
            }
        )
    }
}

/// Configuration pushed from server to client for a new service.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ServicePushConfig {
    pub name: String,
    pub local_addr: String,
    pub service_type: ServiceType,
    pub token: String,
    pub nodelay: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum ControlChannelCmd {
    CreateDataChannel,
    HeartBeat,
    /// Server pushes a new service to the client
    AddService(ServicePushConfig),
    /// Server tells client to remove a service
    RemoveService(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum DataChannelCmd {
    StartForwardTcp,
    StartForwardUdp,
}

type UdpPacketLen = u16; // `u16` should be enough for any practical UDP traffic on the Internet
#[derive(Deserialize, Serialize, Debug)]
struct UdpHeader {
    from: SocketAddr,
    len: UdpPacketLen,
}

#[derive(Debug)]
pub struct UdpTraffic {
    pub from: SocketAddr,
    pub data: Bytes,
}

impl UdpTraffic {
    pub async fn write<T: AsyncWrite + Unpin>(&self, writer: &mut T) -> Result<()> {
        let hdr = UdpHeader {
            from: self.from,
            len: self.data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(&self.data).await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn write_slice<T: AsyncWrite + Unpin>(
        writer: &mut T,
        from: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let hdr = UdpHeader {
            from,
            len: data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(data).await?;

        Ok(())
    }

    pub async fn read<T: AsyncRead + Unpin>(reader: &mut T, hdr_len: u8) -> Result<UdpTraffic> {
        let mut buf = vec![0; hdr_len as usize];
        reader
            .read_exact(&mut buf)
            .await
            .with_context(|| "Failed to read udp header")?;

        let hdr: UdpHeader =
            bincode::deserialize(&buf).with_context(|| "Failed to deserialize UdpHeader")?;

        trace!("hdr {:?}", hdr);

        let mut data = BytesMut::new();
        data.resize(hdr.len as usize, 0);
        reader.read_exact(&mut data).await?;

        Ok(UdpTraffic {
            from: hdr.from,
            data: data.freeze(),
        })
    }
}

pub fn digest(data: &[u8]) -> Digest {
    use sha2::{Digest, Sha256};
    let d = Sha256::new().chain_update(data).finalize();
    d.into()
}

/// Well-known service name for gateway control channels.
/// Clients in gateway mode use this to establish a control channel
/// without needing any pre-configured services.
pub const GATEWAY_SERVICE_NAME: &str = "__gateway__";

/// Prefix/suffix for per-agent gateway service names: `__gw_<agent_id>__`
pub const GATEWAY_PREFIX: &str = "__gw_";
pub const GATEWAY_SUFFIX: &str = "__";

/// Returns the digest for the gateway service.
pub fn gateway_digest() -> Digest {
    digest(GATEWAY_SERVICE_NAME.as_bytes())
}

/// Check if a service name is any kind of gateway (legacy or per-agent).
pub fn is_gateway_service(name: &str) -> bool {
    name == GATEWAY_SERVICE_NAME
        || (name.starts_with(GATEWAY_PREFIX) && name.ends_with(GATEWAY_SUFFIX))
}

/// Extract agent_id from a per-agent gateway name like `__gw_abc123__`.
/// Returns None for legacy `__gateway__` or non-gateway names.
pub fn extract_agent_id(name: &str) -> Option<String> {
    if name.starts_with(GATEWAY_PREFIX) && name.ends_with(GATEWAY_SUFFIX) {
        let inner = &name[GATEWAY_PREFIX.len()..name.len() - GATEWAY_SUFFIX.len()];
        if !inner.is_empty() {
            return Some(inner.to_string());
        }
    }
    None
}

/// Build a per-agent gateway service name.
pub fn agent_gateway_name(agent_id: &str) -> String {
    format!("{}{}{}", GATEWAY_PREFIX, agent_id, GATEWAY_SUFFIX)
}

#[allow(dead_code)]
struct PacketLength {
    hello: usize,
    ack: usize,
    auth: usize,
    c_cmd: usize,
    d_cmd: usize,
}

impl PacketLength {
    pub fn new() -> PacketLength {
        let username = "default";
        let d = digest(username.as_bytes());
        let hello = bincode::serialized_size(&Hello::ControlChannelHello(CURRENT_PROTO_VERSION, d))
            .unwrap() as usize;
        let c_cmd =
            bincode::serialized_size(&ControlChannelCmd::CreateDataChannel).unwrap() as usize;
        let d_cmd = bincode::serialized_size(&DataChannelCmd::StartForwardTcp).unwrap() as usize;
        let ack = Ack::Ok;
        let ack = bincode::serialized_size(&ack).unwrap() as usize;

        let auth = bincode::serialized_size(&Auth(d)).unwrap() as usize;
        PacketLength {
            hello,
            ack,
            auth,
            c_cmd,
            d_cmd,
        }
    }
}

lazy_static! {
    static ref PACKET_LEN: PacketLength = PacketLength::new();
}

pub async fn read_hello<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Hello> {
    let mut buf = vec![0u8; PACKET_LEN.hello];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read hello")?;
    let hello = bincode::deserialize(&buf).with_context(|| "Failed to deserialize hello")?;

    match hello {
        Hello::ControlChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
        Hello::DataChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
    }

    Ok(hello)
}

pub async fn read_auth<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Auth> {
    let mut buf = vec![0u8; PACKET_LEN.auth];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read auth")?;
    bincode::deserialize(&buf).with_context(|| "Failed to deserialize auth")
}

pub async fn read_ack<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Ack> {
    let mut bytes = vec![0u8; PACKET_LEN.ack];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read ack")?;
    bincode::deserialize(&bytes).with_context(|| "Failed to deserialize ack")
}

/// Read a control channel command using length-prefixed framing (v2 protocol).
pub async fn read_control_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<ControlChannelCmd> {
    let len = conn
        .read_u32()
        .await
        .with_context(|| "Failed to read control cmd length")?;
    if len > 1024 * 1024 {
        bail!("Control command too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len as usize];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read control cmd body")?;
    bincode::deserialize(&buf).with_context(|| "Failed to deserialize control cmd")
}

/// Write a control channel command using length-prefixed framing (v2 protocol).
pub async fn write_control_cmd<T: AsyncWrite + Unpin>(
    conn: &mut T,
    cmd: &ControlChannelCmd,
) -> Result<()> {
    let data = bincode::serialize(cmd)?;
    conn.write_u32(data.len() as u32).await?;
    conn.write_all(&data).await?;
    conn.flush().await?;
    Ok(())
}

pub async fn read_data_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<DataChannelCmd> {
    let mut bytes = vec![0u8; PACKET_LEN.d_cmd];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read cmd")?;
    bincode::deserialize(&bytes).with_context(|| "Failed to deserialize data cmd")
}
