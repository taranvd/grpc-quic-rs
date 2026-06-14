//! Service resolver trait and static implementation.

use std::net::SocketAddr;

/// Resolves a service name to one or more socket addresses.
///
/// Implementors can wrap static lists, DNS, consul, etcd, or any discovery
/// backend. The trait is designed to be `Send + Sync + 'static` so resolvers
/// can be stored in shared state and called from async tasks.
pub trait Resolver: Send + Sync + 'static {
    /// Resolve `service_name` to a list of [`SocketAddr`]s.
    ///
    /// Returns an empty `Vec` if the service is not found.
    fn resolve(&self, service_name: &str) -> Vec<SocketAddr>;
}

// ---------------------------------------------------------------------------

/// A [`Resolver`] backed by a static list of `(name, addr)` pairs.
///
/// Useful for testing and single-host deployments.
///
/// ```rust
/// use grpc_quic_discovery::StaticResolver;
///
/// let resolver = StaticResolver::new(vec![
///     ("my-service".to_string(), "127.0.0.1:50051".parse().unwrap()),
/// ]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct StaticResolver {
    entries: Vec<(String, SocketAddr)>,
}

impl StaticResolver {
    /// Create a resolver from an explicit list of `(name, addr)` pairs.
    pub fn new(entries: Vec<(String, SocketAddr)>) -> Self {
        Self { entries }
    }
}

impl Resolver for StaticResolver {
    fn resolve(&self, service_name: &str) -> Vec<SocketAddr> {
        self.entries
            .iter()
            .filter(|(name, _)| name == service_name)
            .map(|(_, addr)| *addr)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_resolver_finds_known_service() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let resolver = StaticResolver::new(vec![("svc-a".to_string(), addr)]);
        let result = resolver.resolve("svc-a");
        assert_eq!(result, vec![addr]);
    }

    #[test]
    fn static_resolver_returns_empty_for_unknown() {
        let resolver = StaticResolver::default();
        assert!(resolver.resolve("unknown").is_empty());
    }
}
