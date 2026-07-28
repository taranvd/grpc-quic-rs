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
                Poll::Ready(Ok(Some(mut buf))) => {
                    let data = buf.copy_to_bytes(buf.remaining());
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
            match this.stream.poll_recv_trailers(cx) {
                Poll::Ready(Ok(Some(trailers))) => {
                    this.trailers_done = true;
                    return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                }
                Poll::Ready(Ok(None)) => {
                    this.trailers_done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Err(e)) => {
                    this.trailers_done = true;
                    return Poll::Ready(Some(Err(CoreError::from(e))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(None)
    }
}

struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

pin_project_lite::pin_project! {
    pub struct ClientRecvBody {
        #[pin]
        stream: h3::client::RequestStream<h3_quinn::RecvStream, Bytes>,
        data_done: bool,
        trailers_done: bool,
        is_closed: bool,
        #[pin]
        err_rx: Option<tokio::sync::oneshot::Receiver<CoreError>>,
        abort_handle: AbortOnDrop,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
    }
}

impl ClientRecvBody {
    pub fn new(
        stream: h3::client::RequestStream<h3_quinn::RecvStream, Bytes>,
        err_rx: Option<tokio::sync::oneshot::Receiver<CoreError>>,
        abort_handle: Option<tokio::task::AbortHandle>,
        deadline: Option<tokio::time::Instant>,
    ) -> Self {
        Self {
            stream,
            data_done: false,
            trailers_done: false,
            is_closed: false,
            err_rx,
            abort_handle: AbortOnDrop(abort_handle),
            deadline: deadline.map(tokio::time::sleep_until),
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
        let mut this = self.project();

        if *this.is_closed {
            return Poll::Ready(None);
        }

        if let Some(deadline) = this.deadline.as_mut().as_pin_mut() {
            if let Poll::Ready(()) = std::future::Future::poll(deadline, cx) {
                *this.is_closed = true;
                if let Some(handle) = this.abort_handle.0.take() {
                    handle.abort();
                }
                if !*this.trailers_done {
                    *this.trailers_done = true;
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", http::HeaderValue::from_static("4"));
                    trailers.insert(
                        "grpc-message",
                        http::HeaderValue::from_static("deadline exceeded"),
                    );
                    return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                } else {
                    return Poll::Ready(None);
                }
            }
        }

        if let Some(rx) = this.err_rx.as_mut().as_pin_mut() {
            match std::future::Future::poll(rx, cx) {
                Poll::Ready(Ok(err)) => {
                    this.err_rx.set(None);
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Err(_)) => {
                    // sender dropped, no error
                    this.err_rx.set(None);
                }
                Poll::Pending => {}
            }
        }

        if !*this.data_done {
            match this.stream.as_mut().poll_recv_data(cx) {
                Poll::Ready(Ok(Some(mut buf))) => {
                    let data = buf.copy_to_bytes(buf.remaining());
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(Ok(None)) => {
                    *this.data_done = true;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(CoreError::from(e))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if !*this.trailers_done {
            match this.stream.as_mut().poll_recv_trailers(cx) {
                Poll::Ready(Ok(Some(trailers))) => {
                    *this.trailers_done = true;
                    return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                }
                Poll::Ready(Ok(None)) => {
                    *this.trailers_done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Err(e)) => {
                    *this.trailers_done = true;
                    return Poll::Ready(Some(Err(CoreError::from(e))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(None)
    }
}
