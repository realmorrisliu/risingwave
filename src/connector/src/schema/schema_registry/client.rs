// Copyright 2024 RisingWave Labs
//
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use futures::future::select_all;
use itertools::Itertools;
use reqwest::{Method, Url};
use serde::de::DeserializeOwned;
use thiserror_ext::AsReport as _;

use super::util::*;
use crate::schema::{invalid_option_error, InvalidOptionError};

pub const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
pub const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";
pub const SCHEMA_REGISTRY_CA_PATH: &str = "schema.registry.ca_pem_path";

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistryAuth {
    username: Option<String>,
    password: Option<String>,
    ca_option: Option<String>,
}

impl From<&HashMap<String, String>> for SchemaRegistryAuth {
    fn from(props: &HashMap<String, String>) -> Self {
        SchemaRegistryAuth {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),
            ca_option: props.get(SCHEMA_REGISTRY_CA_PATH).cloned(),
        }
    }
}

impl From<&BTreeMap<String, String>> for SchemaRegistryAuth {
    fn from(props: &BTreeMap<String, String>) -> Self {
        SchemaRegistryAuth {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),
            ca_option: props.get(SCHEMA_REGISTRY_CA_PATH).cloned(),
        }
    }
}

/// An client for communication with schema registry
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: Vec<Url>,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("all request confluent registry all timeout, {context}\n{}", errs.iter().map(|e| format!("\t{}", e.as_report())).join("\n"))]
pub struct ConcurrentRequestError {
    errs: Vec<itertools::Either<RequestError, tokio::task::JoinError>>,
    context: String,
}

type SrResult<T> = Result<T, ConcurrentRequestError>;

#[derive(thiserror::Error, Debug)]
pub enum SchemaRegistryClientError {
    #[error(transparent)]
    InvalidOption(#[from] InvalidOptionError),
    #[error("read ca file error: {0}")]
    ReadFile(#[source] std::io::Error),
    #[error("parse ca file error: {0}")]
    ParsePem(#[source] reqwest::Error),
    #[error("build schema registry client error: {0}")]
    Build(#[source] reqwest::Error),
}

impl Client {
    pub(crate) fn new(
        url: Vec<Url>,
        client_config: &SchemaRegistryAuth,
    ) -> Result<Self, SchemaRegistryClientError> {
        let valid_urls = url
            .iter()
            .map(|url| (url.cannot_be_a_base(), url))
            .filter(|(x, _)| !*x)
            .map(|(_, url)| url.clone())
            .collect_vec();
        if valid_urls.is_empty() {
            return Err(SchemaRegistryClientError::InvalidOption(
                invalid_option_error!("non-base: {}", url.iter().join(" ")),
            ));
        } else {
            tracing::debug!(
                "schema registry client will use url {:?} to connect",
                valid_urls
            );
        }

        let mut client_builder = reqwest::Client::builder();
        if let Some(ca_path) = client_config.ca_option.as_ref() {
            if ca_path.eq_ignore_ascii_case("ignore") {
                client_builder = client_builder.danger_accept_invalid_certs(true);
            } else {
                client_builder = client_builder.add_root_certificate(
                    reqwest::Certificate::from_pem(
                        &std::fs::read(ca_path).map_err(SchemaRegistryClientError::ReadFile)?,
                    )
                    .map_err(SchemaRegistryClientError::ParsePem)?,
                );
            }
        }

        let inner = client_builder
            .build()
            .map_err(SchemaRegistryClientError::Build)?;

        Ok(Client {
            inner,
            url: valid_urls,
            username: client_config.username.clone(),
            password: client_config.password.clone(),
        })
    }

    async fn concurrent_req<'a, T>(
        &'a self,
        method: Method,
        path: &'a [&'a (impl AsRef<str> + ?Sized + Debug + ToString)],
    ) -> SrResult<T>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let mut fut_req = Vec::with_capacity(self.url.len());
        let mut errs = Vec::with_capacity(self.url.len());
        let ctx = Arc::new(SchemaRegistryCtx {
            username: self.username.clone(),
            password: self.password.clone(),
            client: self.inner.clone(),
            path: path.iter().map(|p| p.to_string()).collect_vec(),
        });
        for url in &self.url {
            fut_req.push(tokio::spawn(req_inner(
                ctx.clone(),
                url.clone(),
                method.clone(),
            )));
        }

        while !fut_req.is_empty() {
            let (result, _index, remaining) = select_all(fut_req).await;
            match result {
                Ok(Ok(res)) => {
                    let _ = remaining.iter().map(|ele| ele.abort());
                    return Ok(res);
                }
                Ok(Err(e)) => errs.push(itertools::Either::Left(e)),
                Err(e) => errs.push(itertools::Either::Right(e)),
            }
            fut_req = remaining;
        }

        Err(ConcurrentRequestError {
            errs,
            context: format!("req path {:?}, urls {}", path, self.url.iter().join(" ")),
        })
    }

    /// get schema by id
    pub async fn get_schema_by_id(&self, id: i32) -> SrResult<ConfluentSchema> {
        let res: GetByIdResp = self
            .concurrent_req(Method::GET, &["schemas", "ids", &id.to_string()])
            .await?;
        Ok(ConfluentSchema {
            id,
            content: res.schema,
        })
    }

    /// get the latest schema of the subject
    pub async fn get_schema_by_subject(&self, subject: &str) -> SrResult<ConfluentSchema> {
        self.get_subject(subject).await.map(|s| s.schema)
    }

    /// get the latest version of the subject
    pub async fn get_subject(&self, subject: &str) -> SrResult<Subject> {
        let res: GetBySubjectResp = self
            .concurrent_req(Method::GET, &["subjects", subject, "versions", "latest"])
            .await?;
        tracing::debug!("update schema: {:?}", res);
        Ok(Subject {
            schema: ConfluentSchema {
                id: res.id,
                content: res.schema,
            },
            version: res.version,
            name: res.subject,
        })
    }

    /// get the latest version of the subject and all it's references(deps)
    pub async fn get_subject_and_references(
        &self,
        subject: &str,
    ) -> SrResult<(Subject, Vec<Subject>)> {
        let mut subjects = vec![];
        let mut visited = HashSet::new();
        let mut queue = vec![(subject.to_owned(), "latest".to_owned())];
        // use bfs to get all references
        while let Some((subject, version)) = queue.pop() {
            let res: GetBySubjectResp = self
                .concurrent_req(Method::GET, &["subjects", &subject, "versions", &version])
                .await?;
            let ref_subject = Subject {
                schema: ConfluentSchema {
                    id: res.id,
                    content: res.schema,
                },
                version: res.version,
                name: res.subject.clone(),
            };
            subjects.push(ref_subject);
            visited.insert(res.subject);
            queue.extend(
                res.references
                    .into_iter()
                    .filter(|r| !visited.contains(&r.subject))
                    .map(|r| (r.subject, r.version.to_string())),
            );
        }
        let origin_subject = subjects.remove(0);

        Ok((origin_subject, subjects))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_get_subject() {
        let url = Url::parse("http://localhost:8081").unwrap();
        let client = Client::new(
            vec![url],
            &SchemaRegistryAuth {
                username: None,
                password: None,
                ca_option: None,
            },
        )
        .unwrap();
        let subject = client
            .get_subject_and_references("proto_c_bin-value")
            .await
            .unwrap();
        println!("{:?}", subject);
    }
}
