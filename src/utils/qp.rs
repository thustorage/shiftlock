use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use rrddmma::{prelude::*, rdma::qp::ExpFeature};
use serde::{Deserialize, Serialize};

/// Out-of-band information pack for QP establishment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteInfo {
    /// Remote QP endpoint.
    pub ep: QpEndpoint,

    /// Remote memory region.
    pub mr: MrRemote,
}

/// Make a QP from scratch on the given device.
pub fn make_qp(dev: impl AsRef<str>, ty: QpType) -> Qp {
    let Nic { context, ports } = Nic::finder()
        .dev_name(dev)
        .probe()
        .expect("cannot find or open device");
    let pd = Pd::new(&context).expect("cannot create PD");
    let cq = Cq::new(&context, Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ");
    let mut qp = Qp::builder()
        .qp_type(ty)
        .caps(match ty {
            QpType::DcIni => QpCaps::for_dc_ini(),
            _ => QpCaps::default(),
        })
        .send_cq(&cq)
        .recv_cq(&cq)
        .sq_sig_all(false)
        .global_routing(false)
        .enable_feature(ExpFeature::ExtendedAtomics)
        .build(&pd)
        .expect("cannot create QP");
    qp.bind_local_port(&ports[0], None)
        .expect("cannot bind local port");
    qp
}

/// Make a QP on the same hardware resources of another QP.
pub fn make_qp_together_with(qp: &Qp, ty: QpType, cq: Option<Cq>) -> Qp {
    let pd = qp.pd();
    let cq = cq
        .unwrap_or_else(|| Cq::new(qp.context(), Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ"));
    let mut another_qp = Qp::builder()
        .qp_type(ty)
        .caps(match ty {
            QpType::DcIni => QpCaps::for_dc_ini(),
            _ => QpCaps::default(),
        })
        .send_cq(&cq)
        .recv_cq(&cq)
        .sq_sig_all(false)
        .global_routing(false)
        .build(pd)
        .expect("cannot create QP");

    let (port, gid_index) = qp.port().unwrap();
    another_qp
        .bind_local_port(port, Some(*gid_index))
        .expect("cannot bind local port");
    another_qp
}

/// Make a DCT on the same hardware resources of another QP.
pub fn make_dct_together_with(qp: &Qp, exp: bool) -> Dct {
    let pd = qp.pd();
    // let cq = Cq::new(qp.context(), Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ");
    let cq = if exp {
        Cq::new_exp(qp.context(), Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ")
    } else {
        Cq::new(qp.context(), Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ")
    };
    let (port, gid_index) = qp.port().unwrap();

    Dct::builder()
        .pd(pd)
        .cq(&cq)
        .port(port, Some(*gid_index))
        .inline_size(64)
        .build(pd.context())
        .expect("cannot create DCT")
}

/// Make a QP connected to the server.
pub fn make_connected_qp(dev: &str, server: SocketAddr) -> (Qp, MrRemote) {
    let mut qp = make_qp(dev, QpType::Rc);
    let ep = qp.endpoint().unwrap();

    let mut socket = TcpStream::connect(server).expect("cannot connect to server");
    {
        let buf = serde_json::to_vec(&ep).expect("cannot serialize QpEndpoint");
        assert!(buf.len() <= 4096, "QpEndpoint too large: {}", buf.len());
        socket
            .write_all(&(buf.len() as u32).to_le_bytes())
            .expect("cannot send length to server");
        socket.write_all(&buf).expect("cannot send to server");
    }

    let remote_info = {
        let mut lenbuf = [0; 4];
        let mut buf = [0; 4096];

        socket
            .read_exact(&mut lenbuf)
            .expect("cannot read length from server");
        let len = u32::from_le_bytes(lenbuf) as usize;
        let buf = &mut buf[..len];
        socket.read_exact(buf).expect("cannot read from server");

        let Ok(remote_info) = serde_json::from_slice::<RemoteInfo>(buf) else {
            panic!(
                "invalid RemoteInfo received: {:?}",
                String::from_utf8_lossy(buf)
            );
        };
        remote_info
    };
    qp.bind_peer(remote_info.ep)
        .expect("cannot bind QP to server peer");
    (qp, remote_info.mr)
}
