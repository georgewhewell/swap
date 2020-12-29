use anyhow::{Result};
use serde::{Serialize, Deserialize};
use reqwest::Client;
use csv::WriterBuilder;

const API: &str = "https://api.bloxy.info";


#[derive(Debug, Serialize, Deserialize)]
pub struct CoinInfo {
    symbol: String,
    name: String,
    address: String,
}

pub async fn fetch_tokens() -> Result<()> {
    let client = Client::builder()
        .user_agent("graphql-rust/0.9.0")
        .build().unwrap();
    
    let uri = format!("{}{}{}", &API.to_string(), "/token/list", "?key=ACCqKRmbsS7R3");
    let res = client.get(&uri).send().await?;
    res.error_for_status_ref()?;

    let v: std::result::Result<Vec<CoinInfo>, reqwest::Error> = res.json().await;

    match v {
        Ok(res) => {
            let mut wtr = WriterBuilder::new().from_path("data/tokens.csv")?;

            for coin in res.into_iter() {
                if !coin.symbol.is_empty() {
                    wtr.serialize(&coin)?;
                }
            }
            wtr.flush()?;
        },
        Err(err) => println!("wtf: {:?}", err)
    }
    
    Ok(())
}