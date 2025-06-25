use anyhow::{Result, anyhow};
use bincode::deserialize;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use csv::Writer;
use log::{error, info, warn, debug};
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::TransactionBinaryEncoding;
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures::stream::{self, StreamExt};

use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{
    TransactionDetails, UiTransactionEncoding, option_serializer::OptionSerializer,
};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::str::FromStr;
use std::path::Path;
use tokio::time::{Duration, sleep};

#[derive(Parser)]
#[command(name = "solend-liquidator-tracker")]
#[command(about = "Track Solend liquidators on Solana blockchain")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan for liquidation transactions
    Scan {
        /// Config file path
        #[arg(long, default_value = "config.toml")]
        config: String,

        /// Override RPC endpoint URL
        #[arg(long)]
        rpc_url: Option<String>,

        /// Override output CSV file path
        #[arg(long)]
        output: Option<String>,

        /// Start from specific signature (optional)
        #[arg(long)]
        before: Option<String>,

        /// Override number of concurrent workers
        #[arg(long)]
        workers: Option<usize>,

        /// Override maximum concurrent RPC requests
        #[arg(long)]
        max_concurrent: Option<usize>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    rpc: RpcConfig,
    scan: ScanConfig,
    programs: ProgramsConfig,
    output: OutputConfig,
    logging: LoggingConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcConfig {
    url: String,
    backup_urls: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ScanConfig {
    batch_size: usize,
    workers: usize,
    max_concurrent: usize,
    batch_delay: u64,
    max_retries: usize,
    retry_delay: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProgramsConfig {
    solend_program_id: String,
    liquidation_discriminators: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OutputConfig {
    default_file: String,
    csv_headers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoggingConfig {
    level: String,
    detailed_logging: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LiquidationEvent {
    signature: String,
    slot: u64,
    timestamp: Option<DateTime<Utc>>,
    liquidator: String,
    program_id: String,
    success: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            rpc: RpcConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                backup_urls: vec![
                    "https://solana-api.projectserum.com".to_string(),
                    "https://api.mainnet-beta.solana.com".to_string(),
                ],
            },
            scan: ScanConfig {
                batch_size: 1000,
                workers: 20,
                max_concurrent: 50,
                batch_delay: 50,
                max_retries: 3,
                retry_delay: 200,
            },
            programs: ProgramsConfig {
                solend_program_id: "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo".to_string(),
                liquidation_discriminators: vec![
                    "9a151f9b4a8c3d8f".to_string(),
                    "e45c3f9c8a2b1d7f".to_string(),
                ],
            },
            output: OutputConfig {
                default_file: "liquidators.csv".to_string(),
                csv_headers: vec![
                    "signature".to_string(),
                    "slot".to_string(),
                    "timestamp".to_string(),
                    "liquidator".to_string(),
                    "program_id".to_string(),
                    "success".to_string(),
                ],
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                detailed_logging: false,
            },
        }
    }
}

fn load_config(path: &str) -> Result<Config> {
    if Path::new(path).exists() {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    } else {
        warn!("配置文件 {} 不存在，使用默认配置", path);
        let default_config = Config::default();
        // 创建默认配置文件
        let toml_string = toml::to_string_pretty(&default_config)?;
        std::fs::write(path, toml_string)?;
        info!("已创建默认配置文件: {}", path);
        Ok(default_config)
    }
}

fn hex_to_bytes(hex_str: &str) -> Result<Vec<u8>> {
    (0..hex_str.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).map_err(|e| anyhow!("解析hex失败: {}", e)))
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Scan {
            config,
            rpc_url,
            output,
            before,
            workers,
            max_concurrent,
        } => {
            let mut cfg = load_config(config)?;
            
            // 命令行参数覆盖配置文件
            if let Some(url) = rpc_url {
                cfg.rpc.url = url.clone();
            }
            if let Some(w) = workers {
                cfg.scan.workers = *w;
            }
            if let Some(mc) = max_concurrent {
                cfg.scan.max_concurrent = *mc;
            }
            
            // 初始化日志
            // std::env::set_var("RUST_LOG", &cfg.logging.level);
            env_logger::init();
            
            let output_file = output.as_ref().unwrap_or(&cfg.output.default_file);
            
            scan_liquidations(&cfg, output_file, before.clone()).await?;
        }
    }

    Ok(())
}

async fn scan_liquidations(
    config: &Config,
    output_path: &str,
    before_signature: Option<String>,
) -> Result<()> {
    let client = Arc::new(RpcClient::new_with_commitment(
        config.rpc.url.clone(), 
        CommitmentConfig::confirmed()
    ));
    let program_id = Pubkey::from_str(&config.programs.solend_program_id)?;
    let semaphore = Arc::new(Semaphore::new(config.scan.max_concurrent));

    // 解析清算指令discriminators
    let discriminators: Result<Vec<Vec<u8>>> = config.programs.liquidation_discriminators
        .iter()
        .map(|hex| hex_to_bytes(hex))
        .collect();
    let discriminators = discriminators?;

    info!("开始扫描Solend清算交易...");
    info!("程序ID: {}", program_id);
    info!("并发工作线程: {}", config.scan.workers);
    info!("最大并发请求: {}", config.scan.max_concurrent);
    info!("批次大小: {}", config.scan.batch_size);

    let mut liquidation_events = Vec::new();
    let mut liquidator_stats: HashMap<String, u32> = HashMap::new();
    let mut before = before_signature
        .map(|s| Signature::from_str(&s))
        .transpose()?;
    let mut total_processed = 0;

    // 创建CSV写入器
    let file = File::create(output_path)?;
    let mut csv_writer = Writer::from_writer(file);
    csv_writer.write_record(&config.output.csv_headers)?;

    loop {
        info!("获取签名批次，限制: {}", config.scan.batch_size);

        let signatures = match client.get_signatures_for_address_with_config(
            &program_id,
            solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                before,
                until: None,
                limit: Some(config.scan.batch_size),
                commitment: Some(CommitmentConfig::confirmed()),
            },
        ) {
            Ok(sigs) => sigs,
            Err(e) => {
                error!("获取签名失败: {}", e);
                break;
            }
        };

        if signatures.is_empty() {
            info!("没有更多签名，扫描完成");
            break;
        }

        info!("处理 {} 个签名", signatures.len());

        // 并发处理批次中的所有签名
        let batch_liquidations = process_signatures_batch(
            &signatures,
            Arc::clone(&client),
            Arc::clone(&semaphore),
            &discriminators,
            config,
        ).await;

        // 处理批次结果
        for liquidation in batch_liquidations {
            *liquidator_stats
                .entry(liquidation.liquidator.clone())
                .or_insert(0) += 1;

            csv_writer.write_record(&[
                &liquidation.signature,
                &liquidation.slot.to_string(),
                &liquidation
                    .timestamp
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_default(),
                &liquidation.liquidator,
                &liquidation.program_id,
                &liquidation.success.to_string(),
            ])?;

            liquidation_events.push(liquidation);
        }

        total_processed += signatures.len();
        before = Some(Signature::from_str(&signatures.last().unwrap().signature)?);

        info!(
            "已处理 {} 个交易，发现 {} 个清算事件",
            total_processed,
            liquidation_events.len()
        );

        sleep(Duration::from_millis(config.scan.batch_delay)).await;
    }

    csv_writer.flush()?;

    // 输出统计信息
    info!("扫描完成！");
    info!("总处理交易数: {}", total_processed);
    info!("总清算事件数: {}", liquidation_events.len());
    info!("独特清算人数: {}", liquidator_stats.len());
    info!("结果已保存到: {}", output_path);

    let mut top_liquidators: Vec<_> = liquidator_stats.iter().collect();
    top_liquidators.sort_by(|a, b| b.1.cmp(a.1));

    println!("\n=== 顶级清算人 ===");
    for (i, (liquidator, count)) in top_liquidators.iter().take(10).enumerate() {
        println!("{}. {} - {} 次清算", i + 1, liquidator, count);
    }

    Ok(())
}

async fn process_signatures_batch(
    signatures: &[solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature],
    client: Arc<RpcClient>,
    semaphore: Arc<Semaphore>,
    discriminators: &[Vec<u8>],
    config: &Config,
) -> Vec<LiquidationEvent> {
    let futures = signatures.iter().map(|sig_info| {
        let signature = sig_info.signature.clone();
        let client = Arc::clone(&client);
        let semaphore = Arc::clone(&semaphore);
        let discriminators = discriminators.to_vec();
        let config_program_id = config.programs.solend_program_id.clone();
        let max_retries = config.scan.max_retries;
        let retry_delay = config.scan.retry_delay;
        
        async move {
            let _permit = semaphore.acquire().await.unwrap();
            
            match Signature::from_str(&signature) {
                Ok(sig) => {
                    match get_transaction_with_retry(&client, &sig, max_retries, retry_delay).await {
                        Ok(Some(transaction)) => {
                            match analyze_transaction(&transaction, &signature, &discriminators, &config_program_id) {
                                Ok(Some(liquidation)) => Some(liquidation),
                                Ok(None) => None,
                                Err(e) => {
                                    warn!("分析交易失败 {}: {}", signature, e);
                                    None
                                }
                            }
                        }
                        Ok(None) => {
                            warn!("交易未找到: {}", signature);
                            None
                        }
                        Err(e) => {
                            warn!("获取交易失败 {}: {}", signature, e);
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!("解析签名失败 {}: {}", signature, e);
                    None
                }
            }
        }
    });

    let results = futures::future::join_all(futures).await;
    results.into_iter().filter_map(|x| x).collect()
}

async fn get_transaction_with_retry(
    client: &RpcClient,
    signature: &Signature,
    max_retries: usize,
    retry_delay: u64,
) -> Result<Option<solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta>> {
    let mut attempt = 0;

    while attempt < max_retries {
        match client.get_transaction_with_config(
            signature,
            solana_client::rpc_config::RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        ) {
            Ok(transaction) => return Ok(Some(transaction)),
            Err(e) => {
                attempt += 1;
                if attempt >= max_retries {
                    return Err(anyhow!(
                        "获取交易失败，重试 {} 次后仍失败: {}",
                        max_retries,
                        e
                    ));
                }
                warn!("获取交易失败，重试 {}/{}: {}", attempt, max_retries, e);
                sleep(Duration::from_millis(retry_delay * attempt as u64)).await;
            }
        }
    }

    Ok(None)
}

fn analyze_transaction(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    signature: &str,
    discriminators: &[Vec<u8>],
    program_id_str: &str,
) -> Result<Option<LiquidationEvent>> {
    let meta = transaction
        .transaction
        .meta
        .as_ref()
        .ok_or_else(|| anyhow!("交易元数据缺失"))?;

    let success = meta.err.is_none();

    let message = match &transaction.transaction.transaction {
        solana_transaction_status::EncodedTransaction::LegacyBinary(data) => {
            let decoded = base64::decode(data)?;
            bincode::deserialize::<solana_sdk::transaction::Transaction>(&decoded)?
        }
        solana_transaction_status::EncodedTransaction::Binary(data, encoding) => match encoding {
            TransactionBinaryEncoding::Base64 => {
                let decoded = base64::decode(data)?;
                deserialize::<Transaction>(&decoded)?
            }
            TransactionBinaryEncoding::Base58 => {
                let decoded = bs58::decode(data).into_vec()?;
                deserialize::<Transaction>(&decoded)?
            }
        },
        _ => return Ok(None),
    };

    let solend_program_id = Pubkey::from_str(program_id_str)?;
    let mut has_solend_call = false;
    let mut is_liquidation = false;

    for instruction in &message.message.instructions {
        let program_id = message
            .message
            .account_keys
            .get(instruction.program_id_index as usize)
            .ok_or_else(|| anyhow!("无效的程序ID索引"))?;

        if *program_id == solend_program_id {
            has_solend_call = true;

            if instruction.data.len() >= 8 {
                let discriminator = &instruction.data[..8];
                for expected_discriminator in discriminators {
                    if discriminator == expected_discriminator.as_slice() {
                        is_liquidation = true;
                        break;
                    }
                }
            }
        }
    }

    if !has_solend_call || !is_liquidation {
        return Ok(None);
    }

    let liquidator = message
        .message
        .account_keys
        .first()
        .ok_or_else(|| anyhow!("没有找到交易发起人"))?
        .to_string();

    let timestamp = transaction
        .block_time
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    Ok(Some(LiquidationEvent {
        signature: signature.to_string(),
        slot: transaction.slot,
        timestamp,
        liquidator,
        program_id: program_id_str.to_string(),
        success,
    }))
}