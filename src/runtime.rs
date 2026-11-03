use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
};
use kube::{Api, Client, api::PostParams};

#[derive(Default)]
pub struct Leadership {
    valid_until: AtomicI64,
}

impl Leadership {
    pub fn is_leader(&self) -> bool {
        chrono::Utc::now().timestamp() < self.valid_until.load(Ordering::SeqCst)
    }

    pub fn renew(&self) {
        self.valid_until.store(
            chrono::Utc::now().timestamp() + 30,
            Ordering::SeqCst,
        );
    }

    pub fn revoke(&self) {
        self.valid_until.store(
            0,
            Ordering::SeqCst,
        );
    }
}

/// Local authority expires before the Kubernetes Lease, leaving enough time
/// for a bounded Catalyst request to finish before another holder takes over.
pub async fn leadership(
    client: Client,
    namespace: String,
    identity: String,
    leader: Arc<Leadership>,
) {
    let api: Api<Lease> = Api::namespaced(
        client, &namespace,
    );

    loop {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            acquire(
                &api, &identity,
            ),
        )
        .await;

        if matches!(
            result,
            Ok(Ok(true))
        ) {
            leader.renew();
        } else {
            leader.revoke();
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn acquire(
    api: &Api<Lease>,
    identity: &str,
) -> Result<bool, kube::Error> {
    let name = "catalyst-operator";
    let now = k8s_openapi::jiff::Timestamp::now();

    if let Some(mut lease) = api.get_opt(name).await? {
        let current = lease.spec.clone().unwrap_or_default();
        let expired = current.renew_time.as_ref().is_none_or(
            |v| {
                (now.as_second() - v.0.as_second())
                    >= i64::from(
                        current
                            .lease_duration_seconds
                            .unwrap_or(60),
                    )
            },
        );

        if current.holder_identity.as_deref() != Some(identity) && !expired {
            return Ok(false);
        }

        lease.spec = Some(
            LeaseSpec {
                holder_identity: Some(identity.into()),
                renew_time: Some(MicroTime(now)),
                lease_duration_seconds: Some(60),
                ..current
            },
        );
        api.replace(
            name,
            &PostParams::default(),
            &lease,
        )
        .await?;
    } else {
        api.create(
            &PostParams::default(),
            &Lease {
                metadata: ObjectMeta {
                    name: Some(name.into()),
                    ..Default::default()
                },
                spec: Some(
                    LeaseSpec {
                        holder_identity: Some(identity.into()),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        lease_duration_seconds: Some(60),
                        ..Default::default()
                    },
                ),
            },
        )
        .await?;
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_requires_a_fresh_lease_and_can_be_revoked() {
        let leadership = Leadership::default();
        assert!(!leadership.is_leader());
        leadership.renew();
        assert!(leadership.is_leader());
        leadership.valid_until.store(
            chrono::Utc::now().timestamp() - 1,
            Ordering::SeqCst,
        );
        assert!(!leadership.is_leader());
        leadership.renew();
        leadership.revoke();
        assert!(!leadership.is_leader());
    }
}
