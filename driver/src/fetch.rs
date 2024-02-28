// Copyright (C) 2024, Tony Rippy
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::io::Read;

use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use http_body_util::{BodyExt, Empty};
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

pub type FetchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn fetch(url: Uri) -> FetchResult<(u64, String)> {
    debug!("starting fetch of {}", url);
    let authority = url.authority().unwrap();
    let host = authority.host();
    let port = authority.port_u16().unwrap_or(80);

    let stream = TcpStream::connect((host, port)).await?;
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            error!("Connection failed: {:?}", err);
        }
    });
    let path = url.path();
    let req = Request::builder()
        .uri(path)
        .header(hyper::header::HOST, authority.as_str())
        .body(Empty::<Bytes>::new())?;

    let res = sender.send_request(req).await?;

    // TODO: This needs real error handling
    debug!("Response: {}", res.status());
    debug!("Headers: {:#?}\n", res.headers());
    let timestamp = match res.headers().get(hyper::header::DATE) {
        Some(date) => {
            let date = date.to_str().unwrap();
            let date = DateTime::parse_from_rfc2822(date).unwrap();
            date.timestamp_millis()
        }
        None => Utc::now().timestamp_millis(),
    };

    // TODO: Verify that this decodes string output correctly.
    // This might only work for UTF-8 ecoded data.
    let mut output = String::new();
    let buf = res.collect().await.unwrap().aggregate();
    buf.reader().read_to_string(&mut output)?;

    Ok((timestamp as u64, output))
}
