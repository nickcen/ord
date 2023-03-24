use log::log;
use {
  anyhow::{anyhow, Result},
  bitcoin::{Transaction, Txid},
  bitcoincore_rpc::Auth,
  hyper::{Body, Client, client::HttpConnector, Method, Request, Uri},
  serde::Deserialize,
  serde_json::{json, Value},
  super::*,
};

pub(crate) struct Fetcher {
  client: Client<HttpConnector>,
  url: Uri,
  auth: String,
}

#[derive(Deserialize, Debug)]
struct JsonResponse<T> {
  result: Option<T>,
  error: Option<JsonError>,
  id: usize,
}

#[derive(Deserialize, Debug)]
struct JsonError {
  code: i32,
  message: String,
}

impl Fetcher {
  pub(crate) fn new(url: &str, auth: Auth) -> Result<Self> {
    if auth == Auth::None {
      return Err(anyhow!("No rpc authentication provided"));
    }

    let client = Client::new();

    let url = if url.starts_with("http://") {
      url.to_string()
    } else {
      "http://".to_string() + url
    };

    let url = Uri::try_from(&url).map_err(|e| anyhow!("Invalid rpc url {url}: {e}"))?;

    let (user, password) = auth.get_user_pass()?;
    let auth = format!("{}:{}", user.unwrap(), password.unwrap());
    let auth = format!("Basic {}", &base64::encode(auth));
    Ok(Fetcher { client, url, auth })
  }

  /// 根据 txid 获取 transaction
  pub(crate) async fn get_transactions(&self, db: DB, txids: Vec<Txid>) -> Result<Vec<Transaction>> {
    if txids.is_empty() {
      return Ok(Vec::new());
    }

    let mut mapping = HashMap::<String, Transaction>::default();

    /// 如果数据库里面加一层 transaction 的表，这里可以不一定要发送json请求
    let mut reqs = Vec::with_capacity(txids.len());
    for (i, txid) in txids.iter().enumerate() {
      match db.get_transaction(txid.to_string()) {
        None => {
          let req = json!({
            "jsonrpc": "2.0",
            "id": i, // Use the index as id, so we can quickly sort the response
            "method": "getrawtransaction",
            "params": [ txid ]
          });
          reqs.push(req);
        }
        Some(t) => {
          mapping.insert(txid.to_string(), t);
        }
      }
    };

    let mut txs = Vec::<Transaction>::with_capacity(txids.len());


    let body = Value::Array(reqs).to_string();
    let req = Request::builder()
      .method(Method::POST)
      .uri(&self.url)
      .header(hyper::header::AUTHORIZATION, &self.auth)
      .header(hyper::header::CONTENT_TYPE, "application/json")
      .body(Body::from(body))?;

    let response = self.client.request(req).await?;

    let buf = hyper::body::to_bytes(response).await?;

    let mut results: Vec<JsonResponse<String>> = serde_json::from_slice(&buf)?;

    // Return early on any error, because we need all results to proceed
    if let Some(err) = results.iter().find_map(|res| res.error.as_ref()) {
      return Err(anyhow!(
        "Failed to fetch raw transaction: code {} message {}",
        err.code,
        err.message
      ));
    }

    // Results from batched JSON-RPC requests can come back in any order, so we must sort them by id
    results.sort_by(|a, b| a.id.cmp(&b.id));

    results
      .into_iter()
      .for_each(|res| {
        res
          .result
          .ok_or_else(|| anyhow!("Missing result for batched JSON-RPC response"))
          .and_then(|str| {
            hex::decode(str)
              .map_err(|e| anyhow!("Result for batched JSON-RPC response not valid hex: {e}"))
          })
          .and_then(|hex| {
            match bitcoin::consensus::deserialize::<Transaction>(&hex) {
              Ok(t) => {
                db.insert_transaction(t.txid().to_string(), &hex);
                mapping.insert(t.txid().to_string(), t.clone());
                Ok(())
              }
              Err(e) => {
                Err(anyhow!("Result for batched JSON-RPC response not valid bitcoin tx: {e}"))
              }
            }
          }).unwrap();
      });


    Ok(txids.iter().map(|txid| {
      mapping.get(&txid.to_string()).unwrap().clone()
    }).collect())
  }
}
