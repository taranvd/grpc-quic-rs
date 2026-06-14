use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use http_body::{Body, Frame};

use crate::error::CoreError;

pin_project_lite::pin_project! {
    pub struct ServerRecvBody {
        #[pin]
        stream: h3::server::RequestStream<h3_quinn::RecvStream, Bytes>,
        data_done: bool,
        trailers_done: bool,
    }
}

impl ServerRecvBody {
    pub fn new(stream: h3::server::RequestStream<h3_quinn::RecvStream, Bytes>) -> Self {
        Self {
            stream,
            data_done: false,
            trailers_done: false,
        }
    }
}

impl Body for ServerRecvBody {
    type Data = Bytes;
    type Error = CoreError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        if !this.data_done {
            match this.stream.poll_recv_data(cx) {
                Poll::Ready(Ok(Some(buf))) => {
                    let data = Bytes::copy_from_slice(buf.chunk());
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(Ok(None)) => {
                    this.data_done = true;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(CoreError::from(e))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if !this.trailers_done {
            this.trailers_done = true;
            return match this.stream.poll_recv_trailers(cx) {
                Poll::Ready(Ok(Some(trailers))) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
                Poll::Ready(Ok(None)) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(CoreError::from(e)))),
                Poll::Pending => Poll::Pending,
            };
        }

        Poll::Ready(None)
    }
}

pin_project_lite::pin_project! {
    pub struct ClientRecvBody {
        #[pin]
        stream: h3::client::RequestStream<h3_quinn::RecvStream, Bytes>,
        data_done: bool,
        trailers_done: bool,
    }
}

impl ClientRecvBody {
    pub fn new(stream: h3::client::RequestStream<h3_quinn::RecvStream, Bytes>) -> Self {
        Self {
            stream,
            data_done: false,
            trailers_done: false,
        }
    }
}

impl Body for ClientRecvBody {
    type Data = Bytes;
    type Error = CoreError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        if !this.data_done {
            match this.stream.poll_recv_data(cx) {
                Poll::Ready(Ok(Some(buf))) => {
                    let data = Bytes::copy_from_slice(buf.chunk());
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(Ok(None)) => {
                    this.data_done = true;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(CoreError::from(e))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if !this.trailers_done {
            this.trailers_done = true;
            return match this.stream.poll_recv_trailers(cx) {
                Poll::Ready(Ok(Some(trailers))) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
                Poll::Ready(Ok(None)) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(CoreError::from(e)))),
                Poll::Pending => Poll::Pending,
            };
        }

        Poll::Ready(None)
    }
}
