use crate::https::HttpsClient;
use clap::ArgMatches;
use std::error::Error;
use hyper::{Body, Request, Response};
use serde_json::{Value};
use url::Url;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::create_https_client;
use crate::error::Error as RestError;

type BoxResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct State {
    pub client: HttpsClient,
    pub url: Url
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Groups {
    pub links: Vec<Link>,
    pub results: Vec<Group>,
    pub total_count: u16
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Link {
    href: String,
    rel: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Group {
    cluster_count: u16,
    created: String,
    id: String,
    links: Vec<Link>,
    name: String,
    org_id: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUsers {
    pub links: Vec<Link>,
    pub results: Vec<User>,
    pub total_count: u16
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Clusters {
    pub links: Vec<Link>,
    pub results: Vec<Cluster>,
    pub total_count: u16
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    name: String
}


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChangeStatus {
    change_status: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    awsIAMType: String,
    database_name: String,
    group_id: String,
    labels: Vec<Value>,
    ldap_auth_type: String,
    links: Vec<Link>,
    roles: Vec<Role>,
    scopes: Vec<Scope>,
    username: String,
    x509_type: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Role {
    database_name: String,
    role_name: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Scope {
    name: String,
    r#type: String
}

impl State {
    pub async fn new(opts: ArgMatches<'_>) -> BoxResult<Self> {
        // Set timeout
        let timeout: u64 = opts
            .value_of("timeout")
            .unwrap()
            .parse()
            .unwrap_or_else(|_| {
                eprintln!("Supplied timeout not in range, defaulting to 60");
                60
            });

        let client = create_https_client(timeout)?;
        let url = opts.value_of("proxy").unwrap().parse().expect("Could not parse url");

        Ok(State {
            client,
            url
        })
    }

    pub async fn get_users(&self, group: &str) -> Result<DatabaseUsers, RestError> {
        let path = format!("groups/{}/databaseUsers?itemsPerPage=500", group);
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value: DatabaseUsers = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get_clusters(&self, group: &str) -> Result<Clusters, RestError> {
        let path = format!("groups/{}/clusters?itemsPerPage=500", group);
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value : Clusters = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get_cluster_status(&self, group: &str, cluster: &str) -> Result<ChangeStatus, RestError> {
        let path = format!("groups/{}/clusters/{}/status", group, cluster);
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value : ChangeStatus = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get_groups(&self) -> Result<Groups, RestError> {
        let path = format!("groups?itemsPerPage=500");
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value : Groups = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get(&self, path: &str) -> Result<Response<Body>, RestError> {

        let uri = format!("{}/{}", &self.url, path);

        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Body::empty())
            .expect("request builder");

        // Send initial request
        let response = match self.client.request(req).await {
            Ok(s) => s,
            Err(e) => {
                log::error!("{{\"error\":\"{}\"", e);
                return Err(RestError::Hyper(e));
            }
        };

        match response.status().as_u16() {
            404 => return Err(RestError::NotFound),
            403 => return Err(RestError::Forbidden),
            401 => return Err(RestError::Unauthorized),
            200 => {
                Ok(response)
            }
            _ => {
                log::error!(
                    "Got bad status code getting config: {}",
                    response.status().as_u16()
                );
                return Err(RestError::UnknownCode)
            }
        }
    }

    pub async fn get_metrics(&self) -> Result<(), RestError> {
        let body = self.get_groups().await?;
        log::debug!("groups: {:?}", body);

        // Create semaphore and vec for handles
        let sem = Arc::new(Semaphore::new(8));
        let mut handles = vec![];

        for group in body.results {
            // Get ticket
            let permit = Arc::clone(&sem).acquire_owned().await;
            let me = self.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;
                log::info!("Getting metrics for group: {}", group.name);

                match me.get_users(group.id.as_str()).await {
                    Ok(database_users) => {
                        let labels = [
                            ("project", group.name.clone()),
                        ];
                        metrics::gauge!("atlas_exporter_project_users_total", database_users.results.len() as f64, &labels);
                        for user in database_users.results {
                            let scopes = match user.scopes.len() {
                                0 => {
                                    let mut vec = Vec::new();
                                    let scope = Scope { name: "all".to_string(), r#type: "CLUSTER".to_string() };
                                    vec.push(scope);
                                    vec
                                },
                                _ => user.scopes.clone()
                            };
            
                            for scope in scopes {
                                let labels = [
                                    ("username", user.username.clone()),
                                    ("project", group.name.clone()),
                                    ("scope", scope.name)
                                ];
                                metrics::gauge!("atlas_exporter_project_user", 1f64, &labels);
                            }
                        }
                    },
                    Err(e) => log::error!("Error getting database users for {}: {}", group.name, e)
                };

                match me.get_clusters(group.id.as_str()).await {
                    Ok(clusters) => {
                        for cluster in clusters.results {
                            log::info!("Getting cluster status for {}:{}", group.name, cluster.name);
                            match me.get_cluster_status(group.id.as_str(), &cluster.name).await {
                                Ok(status) => {
                                    let labels = [
                                            ("project", group.name.clone()),
                                            ("cluster", cluster.name.clone())
                                    ];
                                    let change = match status.change_status.as_str() {
                                        "APPLIED" => 0,
                                        "PENDING" => 1,
                                        _ => 2
                                    };
                                    metrics::gauge!("atlas_exporter_cluster_status", change as f64, &labels);
                                },
                                Err(e) => log::error!("Error getting cluster status for {}, {}: {}", group.name, cluster.name, e)
                            }
                        }
                    },
                    Err(e) => log::error!("Error getting database users for {}: {}", group.name, e)
                }
            }));
        }
        log::info!("Waiting for all futures to complete");
        futures::future::join_all(handles).await;
        log::info!("All futures have completed");

        Ok(())
    }
}
