use anyhow::Result;
use chrono::{Date, Duration, TimeZone, Utc};
use csv::WriterBuilder;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use graphql_client::{GraphQLQuery, Response};
use log::{error, info};
use rust_decimal::prelude::*;
use serde::Serialize;
use std::fs;
use web3::types::Address;

const ENDPOINT: &str = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2";

type BigInt = Decimal;
type BigDecimal = Decimal;
type Bytes = Address;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/schema.json",
    query_path = "gql/swaps.graphql",
    response_derives = "Clone, Debug, Serialize, Deserialize"
)]
pub struct Swaps;

#[derive(Debug, Serialize)]
pub struct FlatRecord {
    pub id: String,
    pub to: Bytes,
    pub sender: Bytes,
    pub log_index: Option<BigInt>,
    pub timestamp: BigInt,
    pub amount0_in: BigDecimal,
    pub amount1_in: BigDecimal,
    pub amount0_out: BigDecimal,
    pub amount1_out: BigDecimal,
    pub amount_usd: BigDecimal,
    pub token0: String,
    pub token1: String,
    pub txn_id: String,
    pub block_number: BigInt,
}

impl FlatRecord {
    pub fn from_swapswap(swap: swaps::SwapsSwaps) -> Self {
        FlatRecord {
            id: swap.id,
            to: swap.to,
            sender: swap.sender,
            log_index: swap.log_index,
            timestamp: swap.timestamp,
            amount0_in: swap.amount0_in,
            amount0_out: swap.amount0_out,
            amount1_in: swap.amount1_in,
            amount1_out: swap.amount1_out,
            amount_usd: swap.amount_usd,
            token0: swap.pair.token0.id,
            token1: swap.pair.token1.id,
            txn_id: swap.transaction.id,
            block_number: swap.transaction.block_number,
        }
    }
}

pub async fn swaps(
    skip: i64,
    timestamp_gte: Option<BigInt>,
    timestamp_lt: Option<BigInt>,
) -> Result<Response<swaps::ResponseData>> {
    let q = Swaps::build_query(swaps::Variables {
        skip: Some(skip),
        start: timestamp_gte,
        end: timestamp_lt,
    });

    let client = reqwest::Client::builder()
        .user_agent("graphql-rust/0.9.0")
        .build()?;

    let res = client.post(ENDPOINT).json(&q).send().await?;

    res.error_for_status_ref()?;

    let response_body: Response<swaps::ResponseData> = res.json().await?;

    Ok(response_body)
}

fn get_filename_for(date: Date<Utc>) -> String {
    date.format("data/%Y/%m/%d.csv").to_string()
}

async fn save_results(date: Date<Utc>, results: Vec<swaps::SwapsSwaps>) -> Result<()> {
    fs::create_dir_all(date.format("data/%Y/%m").to_string())?;
    let mut wtr = WriterBuilder::new().from_path(get_filename_for(date))?;
    for swap in results {
        let rec = FlatRecord::from_swapswap(swap);
        wtr.serialize(&rec)?;
    }
    wtr.flush()?;
    Ok(())
}

pub fn timestamp_of(date: Date<Utc>) -> i64 {
    date.and_hms(0, 0, 0).timestamp()
}

async fn fetch_day(date: Date<Utc>) -> Result<()> {
    let start_time = timestamp_of(date);
    let end_time = timestamp_of(date + Duration::days(1));
    let mut skip = 0;
    let mut running = true;

    let mut results = vec![];

    while running {
        let mut retries = 0;

        while retries < 10 {
            if let Ok(resp) = swaps(skip, Some(start_time.into()), Some(end_time.into())).await {
                if let Some(payload) = resp.data {
                    let num_results = payload.swaps.len();
                    results.extend(payload.swaps);

                    if num_results >= 100 {
                        info!("{}: fetched >= 100 results, page {}", date, skip / 100);
                        skip += num_results as i64;
                    } else {
                        info!("Fetched {} for {}", results.len(), date,);
                        running = false;
                    }

                    break;
                };
            } else {
                error!("Error fetching.. retry {}/10", retries);
                retries += 1;
            }
        }
    }

    save_results(date, results).await?;

    Ok(())
}

async fn ensure_exists(date: Date<Utc>) -> Result<()> {
    if std::path::Path::new(&get_filename_for(date)).exists() {
        info!("Data already exists for {}", date);
        Ok(())
    } else {
        fetch_day(date).await
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut date = Utc.ymd(2020, 5, 17);
    let end_date = Utc::now().date();

    let mut workers = FuturesUnordered::new();

    while workers.len() < 5 {
        workers.push(ensure_exists(date));
        date = date.succ();
    }

    while workers.next().await.is_some() {
        info!("Worker finished");
        if date <= end_date {
            workers.push(ensure_exists(date));
            date = date.succ();
        }
    }
}
