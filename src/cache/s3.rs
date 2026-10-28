// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opendal::Operator;
use opendal::layers::{HttpClientLayer, LoggingLayer, RetryLayer};
use opendal::services::S3;

use crate::errors::*;

use super::http_client::set_user_agent;

pub struct S3Cache {
    bucket: String,
    region: Option<String>,
    key_prefix: String,
    no_credentials: bool,
    endpoint: Option<String>,
    use_ssl: Option<bool>,
    server_side_encryption: Option<bool>,
    server_side_encryption_aws_kms: Option<bool>,
    server_side_encryption_kms_key_id: Option<String>,
    enable_virtual_host_style: Option<bool>,
}

impl S3Cache {
    pub fn new(bucket: String, key_prefix: String, no_credentials: bool) -> Self {
        Self {
            bucket,
            region: None,
            key_prefix,
            no_credentials,
            endpoint: None,
            use_ssl: None,
            server_side_encryption: None,
            server_side_encryption_aws_kms: None,
            server_side_encryption_kms_key_id: None,
            enable_virtual_host_style: None,
        }
    }
    pub fn with_region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }
    pub fn with_use_ssl(mut self, use_ssl: Option<bool>) -> Self {
        self.use_ssl = use_ssl;
        self
    }
    pub fn with_server_side_encryption(mut self, server_side_encryption: Option<bool>) -> Self {
        self.server_side_encryption = server_side_encryption;
        self
    }
    pub fn with_server_side_encryption_aws_kms(
        mut self,
        server_side_encryption_aws_kms: Option<bool>,
    ) -> Self {
        self.server_side_encryption_aws_kms = server_side_encryption_aws_kms;
        self
    }
    pub fn with_server_side_encryption_kms_key_id(
        mut self,
        server_side_encryption_kms_key_id: Option<String>,
    ) -> Self {
        self.server_side_encryption_kms_key_id = server_side_encryption_kms_key_id;
        self
    }
    pub fn with_enable_virtual_host_style(
        mut self,
        enable_virtual_host_style: Option<bool>,
    ) -> Self {
        self.enable_virtual_host_style = enable_virtual_host_style;
        self
    }
    pub fn build(self) -> Result<Operator> {
        let mut builder = S3::default().bucket(&self.bucket).root(&self.key_prefix);

        if let Some(region) = &self.region {
            builder = builder.region(region);
        }

        if let Some(true) = &self.enable_virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }

        if self.no_credentials {
            builder = builder
                .disable_config_load()
                // Disable EC2 metadata to avoid OpenDAL trying to load
                // credentials from EC2 metadata.
                //
                // A.k.a, don't try to visit `http://169.254.169.254`
                .disable_ec2_metadata()
                // Allow anonymous access to S3 so that OpenDAL will not
                // throw error when no credentials are provided.
                .allow_anonymous();
        }

        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint(&endpoint_resolver(endpoint, self.use_ssl)?);
        }

        if let Some(kms_key_id) = &self.server_side_encryption_kms_key_id {
            // SSE-KMS with a customer-managed KMS key.
            builder = builder.server_side_encryption_with_customer_managed_kms_key(kms_key_id);
        } else if self.server_side_encryption_aws_kms.unwrap_or_default() {
            // SSE-KMS with the AWS-managed KMS key (aws/s3).
            builder = builder.server_side_encryption_with_aws_managed_kms_key();
        } else if self.server_side_encryption.unwrap_or_default() {
            // SSE-S3 with an S3-managed key (AES256).
            builder = builder.server_side_encryption_with_s3_key();
        }

        let op = Operator::new(builder)?
            .layer(HttpClientLayer::new(set_user_agent()))
            .layer(LoggingLayer::default())
            // Background L1 writes otherwise lose cache entries on the first
            // temporary disconnect or S3 SlowDown response. Retry only errors
            // OpenDAL marks temporary; persistent failures still reach callers.
            .layer(
                RetryLayer::new()
                    .with_max_times(3)
                    .with_min_delay(std::time::Duration::from_millis(100))
                    .with_max_delay(std::time::Duration::from_secs(1))
                    .with_jitter(),
            )
            .finish();
        Ok(op)
    }
}

/// Resolve given endpoint along with use_ssl settings.
fn endpoint_resolver(endpoint: &str, use_ssl: Option<bool>) -> Result<String> {
    let endpoint_uri: http::Uri = endpoint
        .try_into()
        .map_err(|err| anyhow!("input endpoint {endpoint} is invalid: {:?}", err))?;
    let mut parts = endpoint_uri.into_parts();
    match use_ssl {
        Some(true) => {
            parts.scheme = Some(http::uri::Scheme::HTTPS);
        }
        Some(false) => {
            parts.scheme = Some(http::uri::Scheme::HTTP);
        }
        None => {
            if parts.scheme.is_none() {
                parts.scheme = Some(http::uri::Scheme::HTTP);
            }
        }
    }
    // path_and_query is required when scheme is set
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));
    }

    Ok(http::Uri::from_parts(parts)?.to_string())
}

#[cfg(test)]
mod test {
    use super::*;

    // A local HTTP endpoint exercises the actual S3 operator/retry stack.
    // None closes a fully received request without sending a response.
    async fn upload_with_responses(responses: Vec<Option<u16>>) -> (bool, usize) {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let i = observed.fetch_add(1, Ordering::SeqCst);
                let status = responses[i.min(responses.len() - 1)];
                if let Some(status) = status {
                    let (reason, body) = match status {
                        200 => ("OK", ""),
                        403 => ("Forbidden", "<Error><Code>AccessDenied</Code></Error>"),
                        _ => (
                            "Service Unavailable",
                            "<Error><Code>SlowDown</Code></Error>",
                        ),
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nETag: \"test\"\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
            }
        });
        let op = S3Cache::new("test-bucket".into(), "test/".into(), true)
            .with_region(Some("us-east-1".into()))
            .with_endpoint(Some(format!("http://{address}")))
            .with_use_ssl(Some(false))
            .build()
            .unwrap();
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), op.write("key", "payload")).await;
        server.abort();
        (outcome.unwrap().is_ok(), attempts.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn test_s3_retries_temporary_upload_failures() {
        assert_eq!(
            upload_with_responses(vec![Some(503), Some(200)]).await,
            (true, 2)
        );
        assert_eq!(
            upload_with_responses(vec![None, Some(200)]).await,
            (true, 2)
        );
    }

    #[tokio::test]
    async fn test_s3_retry_budget_and_permanent_errors() {
        assert_eq!(upload_with_responses(vec![Some(503)]).await, (false, 4));
        assert_eq!(upload_with_responses(vec![Some(403)]).await, (false, 1));
    }

    #[test]
    fn test_endpoint_resolver() -> Result<()> {
        let cases = vec![
            (
                "no scheme without use_ssl",
                "s3-us-east-1.amazonaws.com",
                None,
                "http://s3-us-east-1.amazonaws.com/",
            ),
            (
                "http without use_ssl",
                "http://s3-us-east-1.amazonaws.com",
                None,
                "http://s3-us-east-1.amazonaws.com/",
            ),
            (
                "https without use_ssl",
                "https://s3-us-east-1.amazonaws.com",
                None,
                "https://s3-us-east-1.amazonaws.com/",
            ),
            (
                "no scheme with use_ssl",
                "s3-us-east-1.amazonaws.com",
                Some(true),
                "https://s3-us-east-1.amazonaws.com/",
            ),
            (
                "http with use_ssl",
                "http://s3-us-east-1.amazonaws.com",
                Some(true),
                "https://s3-us-east-1.amazonaws.com/",
            ),
            (
                "https with use_ssl",
                "https://s3-us-east-1.amazonaws.com",
                Some(true),
                "https://s3-us-east-1.amazonaws.com/",
            ),
            (
                "no scheme with not use_ssl",
                "s3-us-east-1.amazonaws.com",
                Some(false),
                "http://s3-us-east-1.amazonaws.com/",
            ),
            (
                "http with not use_ssl",
                "http://s3-us-east-1.amazonaws.com",
                Some(false),
                "http://s3-us-east-1.amazonaws.com/",
            ),
            (
                "https with not use_ssl",
                "https://s3-us-east-1.amazonaws.com",
                Some(false),
                "http://s3-us-east-1.amazonaws.com/",
            ),
        ];

        for (name, endpoint, use_ssl, expected) in cases {
            let actual = endpoint_resolver(endpoint, use_ssl)?;
            assert_eq!(actual, expected, "{}", name);
        }

        Ok(())
    }
}
