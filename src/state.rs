use crate::https::HttpsClient;
use clap::ArgMatches;
use std::error::Error;
use hyper::{Body, Request, Response};
//use serde_json::{Value};
use url::Url;
use serde::{Deserialize, Serialize};
use chrono::{Utc, SecondsFormat};
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Duration;

use crate::create_https_client;
use crate::error::Error as RestError;

type BoxResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataV2 {
    total_cost: f64,
    deployments: Vec<Deployment>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Deployment {
    deployment_id: String,
    deployment_name: String,
    costs: Cost,
    hourly_rate: f64,
    period: Period
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Cost {
    total: f64,
    dimensions: Vec<Item>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item {
    r#type: String,
    cost: f64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Period {
    start: String,
    end: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Data{
    data: Vec<Inner>
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
        let url = opts.value_of("url").unwrap().parse().expect("Could not parse url");

        Ok(State {
            client,
            url
        })
    }

    pub async fn get_deployments(&self) -> Result<DataV2, RestError> {
        let now = Utc::now();
        let month_start = Utc.ymd(now.year(), now.month(), 1).and_hms(0,0,0);

        let path = format!("deployments?from={}", month_start.to_rfc3339_opts(SecondsFormat::Secs, true));
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value: DataV2 = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get_charts(&self) -> Result<Data, RestError> {
        let now = Utc::now();
        let hour_ago = Utc::now() - Duration::hours(1);
        let path = format!("charts?from={}&to={}", hour_ago.to_rfc3339_opts(SecondsFormat::Secs, true), now.to_rfc3339_opts(SecondsFormat::Secs, true));
        let body = self.get(&path).await?;
        let bytes = hyper::body::to_bytes(body.into_body()).await?;
        let value: Data = serde_json::from_slice(&bytes)?;
        Ok(value)
    }

    pub async fn get(&self, path: &str) -> Result<Response<Body>, RestError> {
        let uri = format!("{}/{}", &self.url, path);
        log::debug!("getting url {}", &uri);
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
        let deployments = self.get_deployments().await?;
        log::debug!("deployments: {:?}", deployments);

        let charts = self.get_charts().await?;
        log::debug!("charts: {:?}", charts);

        // Get hourly data
        for cluster in &charts.data[0].values {
            let labels = [
                ("id", cluster.id.clone()),
                ("name", cluster.name.clone()),
            ];
            log::debug!("Adding metric: elastic_billing_hourly_rate, labels: {:?}, value: {}", &labels, cluster.value.clone());
            metrics::gauge!("elastic_billing_hourly_rate", cluster.value.clone(), &labels);
        }

        // Get monthly data
        for deployment in &deployments.deployments {
            let labels = [
                ("id", deployment.deployment_id.clone()),
                ("name", deployment.deployment_name.clone()),
            ];
            log::debug!("Adding metric: elastic_billing_monthly_cost_total, labels: {:?}, value: {}", &labels, deployment.costs.total.clone());
            metrics::gauge!("elastic_billing_monthly_cost_total", deployment.costs.total.clone(), &labels);


            log::debug!("Adding metric: elastic_billing_monthly_hourly_rate, labels: {:?}, value: {}", &labels, deployment.hourly_rate.clone());
            metrics::gauge!("elastic_billing_monthly_hourly_rate", deployment.hourly_rate.clone(), &labels);

            for item in &deployment.costs.dimensions {
                let labels = [
                    ("id", deployment.deployment_id.clone()),
                    ("name", deployment.deployment_name.clone()),
                    ("item", item.r#type.clone()),
                ];
                log::debug!("Adding metric: elastic_billing_itemized_monthly_cost_total, labels: {:?}, value: {}", &labels, item.cost.clone());
                metrics::gauge!("elastic_billing_itemized_monthly_cost_total", item.cost.clone(), &labels);
            }

        }
        Ok(())
    }
}
