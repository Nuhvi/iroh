//! Utility for rpc interactions
use std::marker::PhantomData;

use futures::{
    future::{self, BoxFuture, ErrInto},
    Future, FutureExt, SinkExt, StreamExt, TryFuture, TryFutureExt,
};
use quic_rpc::{
    client::RpcClientError,
    message::{InteractionPattern, Msg},
    server::{RpcChannel, RpcServerError},
    RpcClient, Service, ServiceConnection, ServiceEndpoint,
};

use crate::util::RpcError;

/// Interaction pattern for rpc messages that can report progress
#[derive(Debug, Clone, Copy)]
pub struct RpcWithProgress;

impl InteractionPattern for RpcWithProgress {}

/// A rpc message with progress updates
///
/// This can be useful for long running operations where the client wants to
/// display progress to the user.
pub trait RpcWithProgressMsg<S: Service>: Msg<S> {
    /// The final response
    type Response: Into<S::Res> + TryFrom<S::Res> + Send + 'static;
    /// The self contained progress updates
    type Progress: Into<S::Res> + TryFromOrKeep<S::Res> + Send + 'static;
}

/// Helper trait to attempt a conversion and keep the original value if it fails
///
/// To implement this you have to implement TryFrom for the type and the reference.
/// This can be done with derive_more using #[derive(TryInto)].
pub trait TryFromOrKeep<T>: TryFrom<T> + Sized {
    /// Convert the value or keep it if it can't be converted
    fn convert_or_keep(s: T) -> std::result::Result<Self, T>;
}

impl<T, U> TryFromOrKeep<U> for T
where
    for<'a> &'a Self: TryFrom<&'a U>,
    Self: TryFrom<U>,
{
    fn convert_or_keep(x: U) -> std::result::Result<Self, U> {
        let can_convert = (<&Self>::try_from(&x)).is_ok();
        if can_convert {
            Ok(Self::try_from(x)
                .unwrap_or_else(|_| panic!("TryFrom inconsistent for byref and byval")))
        } else {
            Err(x)
        }
    }
}

/// Extension trait for rpc clients that adds rpc_with_progress helper method
pub trait RpcClientExt<S: Service, C: ServiceConnection<S>> {
    /// Perform a rpc call that returns a stream of progress updates.
    ///
    /// The progress updates are sent to the progress callback.
    /// Delivery is not guaranteed for progress updates, so the progress
    /// message should be self contained.
    ///
    /// Also, progress updates will be dropped on the client side if the client
    /// is not fast enough to process them.
    fn rpc_with_progress<M, F, Fut>(
        &self,
        msg: M,
        progress: F,
    ) -> BoxFuture<'_, std::result::Result<M::Response, RpcClientError<C>>>
    where
        M: RpcWithProgressMsg<S>,
        F: (Fn(M::Progress) -> Fut) + Send + 'static,
        Fut: Future<Output = ()> + Send;
}

impl<S: Service, C: ServiceConnection<S>> RpcClientExt<S, C> for RpcClient<S, C> {
    fn rpc_with_progress<M, F, Fut>(
        &self,
        msg: M,
        progress: F,
    ) -> BoxFuture<'_, std::result::Result<M::Response, RpcClientError<C>>>
    where
        M: RpcWithProgressMsg<S>,
        F: (Fn(M::Progress) -> Fut) + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (psend, mut precv) = tokio::sync::mpsc::channel(5);
        // future that does the call and filters the results
        let worker = async move {
            let (mut send, mut recv) = self
                .as_ref()
                .open_bi()
                .await
                .map_err(RpcClientError::Open)?;
            send.send(msg.into())
                .await
                .map_err(RpcClientError::<C>::Send)?;
            // read the results
            while let Some(msg) = recv.next().await {
                // handle recv errors
                let msg = msg.map_err(RpcClientError::RecvError)?;
                // first check if it is a progress message
                match M::Progress::convert_or_keep(msg) {
                    Ok(p) => {
                        // we got a progress message. Pass it on.
                        if psend.try_send(p).is_err() {
                            tracing::debug!("progress message dropped");
                        }
                    }
                    Err(msg) => {
                        // we got the response message.
                        // Convert it to the response type and return it.
                        // if the conversion fails, return an error.
                        let res = M::Response::try_from(msg)
                            .map_err(|_| RpcClientError::DowncastError)?;
                        return Ok(res);
                    }
                }
            }
            // The server closed the connection before sending a response.
            Err(RpcClientError::EarlyClose)
        };
        let forwarder = async move {
            // forward progress messages to the client.
            while let Some(msg) = precv.recv().await {
                let f = progress(msg);
                f.await;
            }
            // wait forever. we want the other future to complete first.
            future::pending::<std::result::Result<M::Response, RpcClientError<C>>>().await
        };
        async move {
            tokio::select! {
                res = worker => res,
                res = forwarder => res,
            }
        }
        .boxed()
    }
}

/// Extension trait for rpc channels that adds rpc_with_progress helper method
pub trait RpcChannelExt<S: Service, C: ServiceEndpoint<S>> {
    /// Perform an rpc that will send progress messages to the client.
    ///
    /// Progress messages, unlike other messages, can be dropped if the client
    /// is not ready to receive them. So the progress messages should be
    /// self-contained.
    fn rpc_with_progress<M, T, F, Fut>(
        self,
        msg: M,
        target: T,
        f: F,
    ) -> BoxFuture<'static, std::result::Result<(), RpcServerError<C>>>
    where
        M: RpcWithProgressMsg<S>,
        F: FnOnceTrait<(T, M, Box<dyn Fn(M::Progress) + Send>), Fut> + Send + 'static,
        // F: FnOnce(T, M, Box<dyn Fn(M::Progress) + Send>) -> Fut + Send + 'static,
        Fut: Future<Output = M::Response> + Send,
        T: Send + 'static;
}

impl<S: Service, C: ServiceEndpoint<S>> RpcChannelExt<S, C> for RpcChannel<S, C> {
    fn rpc_with_progress<M, T, F, Fut>(
        mut self,
        msg: M,
        target: T,
        f: F,
    ) -> BoxFuture<'static, std::result::Result<(), RpcServerError<C>>>
    where
        M: RpcWithProgressMsg<S>,
        F: FnOnceTrait<(T, M, Box<dyn Fn(M::Progress) + Send>), Fut> + Send + 'static,
        // F: FnOnce(T, M, Box<dyn Fn(M::Progress) + Send>) -> Fut + Send + 'static,
        Fut: Future<Output = M::Response> + Send,
        T: Send + 'static,
    {
        let (send, mut recv) = tokio::sync::mpsc::channel(5);
        let send2 = send.clone();

        // call the function that will actually perform the rpc, then send the result
        // to the forwarder, which will forward it to the rpc channel.
        let fut = async move {
            let progress: Box<dyn Fn(M::Progress) + Send> = Box::new(move |progress| {
                if send.try_send(Err(progress)).is_err() {
                    // The forwarder is not ready to receive the message.
                    // Drop it, it is just a self contained progress message.
                    tracing::debug!("progress message dropped");
                }
            });
            let res = f.call((target, msg, progress)).await;
            send2
                .send(Ok(res))
                .await
                .map_err(|_| RpcServerError::EarlyClose)?;
            // wait forever. we want the other future to complete first.
            future::pending::<std::result::Result<(), RpcServerError<C>>>().await
        };
        // forwarder future that will forward messages to the rpc channel.
        let forwarder = async move {
            // this will run until the rpc future completes and drops the send.
            while let Some(msg) = recv.recv().await {
                // if we get a response, we are immediately done.
                let done = msg.is_ok();
                let msg = match msg {
                    Ok(msg) => msg.into(),
                    Err(msg) => msg.into(),
                };
                self.send
                    .send(msg)
                    .await
                    .map_err(RpcServerError::SendError)?;
                if done {
                    break;
                }
            }
            Ok(())
        };
        // wait until either the rpc future or the forwarder future completes.
        //
        // the forwarder will complete only when there is a send error.
        // the rpc future will complete when the rpc is done.
        async move {
            tokio::select! {
                res = fut => res,
                res = forwarder => res,
            }
        }
        .boxed()
    }
}

/// Trait to abstract over functions with different arity.
///
/// Until https://github.com/rust-lang/rust/issues/29625 is implemented
pub trait FnOnceTrait<A, R> {
    /// Call the function with the given arguments.
    fn call(self, a: A) -> R;
}
impl<R, T: FnOnce() -> R> FnOnceTrait<(), R> for T {
    fn call(self, _: ()) -> R {
        (self)()
    }
}
impl<A, R, T: FnOnce(A) -> R> FnOnceTrait<(A,), R> for T {
    fn call(self, a: (A,)) -> R {
        (self)(a.0)
    }
}
impl<A, B, R, T: FnOnce(A, B) -> R> FnOnceTrait<(A, B), R> for T {
    fn call(self, a: (A, B)) -> R {
        (self)(a.0, a.1)
    }
}
impl<A, B, C, R, T: FnOnce(A, B, C) -> R> FnOnceTrait<(A, B, C), R> for T {
    fn call(self, a: (A, B, C)) -> R {
        (self)(a.0, a.1, a.2)
    }
}
impl<A, B, C, D, R, T: FnOnce(A, B, C, D) -> R> FnOnceTrait<(A, B, C, D), R> for T {
    fn call(self, a: (A, B, C, D)) -> R {
        (self)(a.0, a.1, a.2, a.3)
    }
}

/// Take a function that produces a [TryFuture] and transform the result
/// so that the error is wrapped in an [RpcError] so that it is serializable.
pub const fn err_into_rpc_error<A, Fut, T>(value: T) -> impl FnOnceTrait<A, ErrInto<Fut, RpcError>>
where
    T: FnOnceTrait<A, Fut> + Send,
    Fut: TryFuture + Send,
    RpcError: From<Fut::Error>,
    A: Send,
{
    FnOnceErrInto {
        inner: value,
        p: PhantomData,
    }
}

struct FnOnceErrInto<Fut, E, T> {
    inner: T,
    p: PhantomData<(Fut, E)>,
}

impl<A, Fut, T> FnOnceTrait<A, ErrInto<Fut, RpcError>> for FnOnceErrInto<A, Fut, T>
where
    T: FnOnceTrait<A, Fut> + Send,
    Fut: TryFuture + Send,
    RpcError: From<Fut::Error>,
    A: Send,
{
    fn call(self, a: A) -> ErrInto<Fut, RpcError> {
        self.inner.call(a).err_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_transform() {
        let a = || async move { anyhow::Ok(()) };
        let b = |x: u64| async move { anyhow::Ok(x) };
        let c = |x: u64, y: u64| async move { anyhow::Ok((x, y)) };
        let _a = err_into_rpc_error(a);
        let _b = err_into_rpc_error(b);
        let _c = err_into_rpc_error(c);
    }
}
