use super::session::{LoginParameters, RithmicSession};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

type RithmicSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) async fn connect(
    url: &str,
    login: LoginParameters,
    response_timeout: Duration,
) -> Result<(RithmicSocket, RithmicSession)> {
    let mut session = RithmicSession::new(login);

    let (mut discovery, _) = timeout(response_timeout, connect_async(url))
        .await
        .context("Rithmic system-info connection timed out")??;
    send_binary(&mut discovery, session.begin_system_info()?).await?;
    let response = receive_binary(&mut discovery, response_timeout).await?;
    session.accept_system_info(&response)?;
    drop(discovery);

    let (mut socket, _) = timeout(response_timeout, connect_async(url))
        .await
        .context("Rithmic login connection timed out")??;
    session.mark_reconnected()?;
    send_binary(&mut socket, session.begin_login()?).await?;
    let response = receive_binary(&mut socket, response_timeout).await?;
    let initial_heartbeat = session.accept_login(&response)?;
    send_binary(&mut socket, initial_heartbeat).await?;

    Ok((socket, session))
}

async fn send_binary(socket: &mut RithmicSocket, payload: Vec<u8>) -> Result<()> {
    socket.send(Message::Binary(payload.into())).await?;
    Ok(())
}

async fn receive_binary(socket: &mut RithmicSocket, wait: Duration) -> Result<Vec<u8>> {
    timeout(wait, async {
        while let Some(message) = socket.next().await {
            match message? {
                Message::Binary(payload) => return Ok(payload.to_vec()),
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                Message::Close(_) => bail!("Rithmic closed before sending a response"),
                _ => bail!("Rithmic sent a non-binary protocol message"),
            }
        }
        bail!("Rithmic connection ended before sending a response")
    })
    .await
    .context("Rithmic response timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::rithmic::session::{Plant, SessionState};
    use crate::connector::rithmic::{codec, protocol};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn connects_system_info_then_login_and_sends_initial_heartbeat() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut discovery = accept_async(stream).await.unwrap();
            assert_template(discovery.next().await.unwrap().unwrap(), 16);
            discovery
                .send(Message::Binary(
                    codec::encode(&protocol::ResponseRithmicSystemInfo {
                        template_id: 17,
                        rp_code: vec!["0".to_string()],
                        system_name: vec!["test-system".to_string()],
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            discovery.close(None).await.unwrap();

            let (stream, _) = listener.accept().await.unwrap();
            let mut login = accept_async(stream).await.unwrap();
            assert_template(login.next().await.unwrap().unwrap(), 10);
            login
                .send(Message::Binary(
                    codec::encode(&protocol::ResponseLogin {
                        template_id: 11,
                        rp_code: vec!["0".to_string()],
                        heartbeat_interval: Some(30.0),
                        ..Default::default()
                    })
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
            assert_template(login.next().await.unwrap().unwrap(), 18);
        });

        let login = LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Ticker,
        )
        .unwrap();
        let (_, session) = connect(&url, login, Duration::from_secs(1)).await.unwrap();

        assert_eq!(session.state(), SessionState::Active);
        server.await.unwrap();
    }

    fn assert_template(message: Message, expected: i32) {
        let Message::Binary(payload) = message else {
            panic!("expected binary Rithmic message");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), expected);
    }
}
