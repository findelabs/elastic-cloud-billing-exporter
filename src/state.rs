use crate::https::HttpsClient;
use clap::ArgMatches;
use std::error::Error;
use hyper::{Body, Request, Response};
//use serde_json::{Value};
use url::Url;
use serde::{Deserialize, Serialize};
use chrono::{Duration, Utc};

use crate::create_https_client;
use crate::error::Error as RestError;

type BoxResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Data{
    array: Vec<Inner>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Inner {
    pub timestamp: u64,
    pub values: Vec<Cluster>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Cluster {
    id: String,
    name: String,
    value: f64
}

#[derive(Clone, Debug)]
pub struct State {
    pub client: HttpsClient,
    pub url: Url
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
        let url = opts.value_of("org").unwrap().parse().expect("Could not parse url");

        Ok(State {
            client,
            url
        })
    }

    pub async fn get_clusters(&self) -> Result<Data, RestError> {
        let hour_ago = Utc::now() + Duration::hours(1);
        let path = format!("{}/charts?=from", hour_ago);
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value : Data = serde_json::from_slice(&bytes)?;
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
        let body = self.get_clusters().await?;
        log::debug!("response: {:?}", body);
        
        for cluster in &body.array[0].values {
            let labels = [
                ("id", cluster.id.clone()),
                ("name", cluster.name.clone()),
            ];
            metrics::gauge!("elastic_billing_exporter_hourly_cost", cluster.value.clone(), &labels);
        }
        Ok(())
    }
}
