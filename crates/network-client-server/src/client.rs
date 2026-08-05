use crate::{
    ChallengeRequest, ChallengeResponse, ClientServerConfig, CommitRequest, ControlAcceptance,
    ControlFrame, PublishAcceptance, PublishFrame, SessionProofRequest, SessionResponse,
};
use anyhow::{Context, Result, bail};
use reqwest::blocking::{Client, RequestBuilder};
use wattswarm_network_transport_core::{DeliveryClass, DeliveryPage};

pub trait ClientServerTransport: Send + Sync {
    fn challenge(&self, request: &ChallengeRequest) -> Result<ChallengeResponse>;
    fn prove_session(&self, request: &SessionProofRequest) -> Result<SessionResponse>;
    fn publish(&self, session_token: &str, frame: &PublishFrame) -> Result<PublishAcceptance>;
    fn send_control(&self, session_token: &str, frame: &ControlFrame) -> Result<ControlAcceptance>;
    fn pull_page(
        &self,
        session_token: &str,
        delivery_class: DeliveryClass,
    ) -> Result<Option<DeliveryPage>>;
    fn commit(&self, session_token: &str, request: &CommitRequest) -> Result<()>;
}

pub struct ClientServerClient {
    config: ClientServerConfig,
    http: Client,
}

impl ClientServerClient {
    pub fn new(config: ClientServerConfig) -> Result<Self> {
        config.validate()?;
        let http = Client::builder().timeout(config.request_timeout).build()?;
        Ok(Self { config, http })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.config.gateway_url.trim_end_matches('/'), path)
    }

    fn authenticated(&self, token: &str, request: RequestBuilder) -> Result<RequestBuilder> {
        if token.trim().is_empty() {
            bail!("ClientServer session token cannot be empty");
        }
        Ok(request.bearer_auth(token))
    }
}

impl ClientServerTransport for ClientServerClient {
    fn challenge(&self, request: &ChallengeRequest) -> Result<ChallengeResponse> {
        if request.principals.len() != 1 {
            bail!("ClientServer V1 requires exactly one logical principal");
        }
        Ok(self
            .http
            .post(self.endpoint("v1/session/challenge"))
            .json(request)
            .send()?
            .error_for_status()?
            .json()?)
    }

    fn prove_session(&self, request: &SessionProofRequest) -> Result<SessionResponse> {
        if request.principals.len() != 1
            || request.proofs.len() != 1
            || request.principals[0].principal_id != request.proofs[0].principal_id
        {
            bail!("ClientServer V1 requires one matching principal proof");
        }
        if request.delivery_policy_version != self.config.delivery_policy_version {
            bail!("ClientServer delivery policy version mismatch");
        }
        let response: SessionResponse = self
            .http
            .post(self.endpoint("v1/session/proof"))
            .json(request)
            .send()?
            .error_for_status()?
            .json()?;
        if response.delivery_policy_version != self.config.delivery_policy_version {
            bail!("Gateway delivery policy version mismatch");
        }
        Ok(response)
    }

    fn publish(&self, session_token: &str, frame: &PublishFrame) -> Result<PublishAcceptance> {
        if frame.delivery_policy_version != self.config.delivery_policy_version {
            bail!("publish delivery policy version mismatch");
        }
        let request = self.authenticated(
            session_token,
            self.http.post(self.endpoint("v1/publish")).json(frame),
        )?;
        let acceptance: PublishAcceptance = request.send()?.error_for_status()?.json()?;
        if acceptance.record_id != frame.record_id
            || acceptance.delivery_policy_version != self.config.delivery_policy_version
        {
            bail!("Gateway publish acceptance does not match request");
        }
        Ok(acceptance)
    }

    fn send_control(&self, session_token: &str, frame: &ControlFrame) -> Result<ControlAcceptance> {
        let request = self.authenticated(
            session_token,
            self.http.post(self.endpoint("v1/control")).json(frame),
        )?;
        let acceptance: ControlAcceptance = request.send()?.error_for_status()?.json()?;
        if acceptance.correlation_id != frame.correlation_id {
            bail!("Gateway control acceptance does not match request");
        }
        Ok(acceptance)
    }

    fn pull_page(
        &self,
        session_token: &str,
        delivery_class: DeliveryClass,
    ) -> Result<Option<DeliveryPage>> {
        let request = self.authenticated(
            session_token,
            self.http.get(self.endpoint("v1/mailbox/page")).query(&[
                ("delivery_class", delivery_class.as_str()),
                ("limit", &self.config.delivery_page_size.to_string()),
            ]),
        )?;
        let response = request.send()?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let page: DeliveryPage = response.error_for_status()?.json()?;
        page.validate().context("invalid Gateway delivery page")?;
        if page.binding.delivery_class != delivery_class {
            bail!("Gateway returned a cross-class delivery page");
        }
        Ok(Some(page))
    }

    fn commit(&self, session_token: &str, request: &CommitRequest) -> Result<()> {
        self.authenticated(
            session_token,
            self.http
                .post(self.endpoint("v1/mailbox/commit"))
                .json(request),
        )?
        .send()?
        .error_for_status()?;
        Ok(())
    }
}
