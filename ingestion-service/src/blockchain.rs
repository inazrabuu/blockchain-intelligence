use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::network::TransactionResponse;
use alloy::consensus::Transaction;
use futures_util::StreamExt;

pub async fn subscribe_blocks(
    ws_url: &str,
    http_url: &str,
    tx: tokio::sync::mpsc::Sender<shared::transaction::Transaction>
) -> Result<(), Box<dyn std::error::Error>> {
    // Websocket provider
    let ws = WsConnect::new(ws_url);

    let ws_provider = match ProviderBuilder::new()
        .connect_ws(ws)
        .await
    {
        Ok(provider) => provider,
        Err(err) => {
            println!("Ethereum Websocket conn failed {:?}", err);
            return Err(err.into());
        }
    };

    println!("Connected to Ethereum WebSocket");

    // HTTP Provider
    let http_provider = ProviderBuilder::new()
        .connect_http(http_url.parse()?);

    println!("Connected to Ethereum HTTP");

    let subscription = ws_provider.subscribe_blocks().await?;

    let mut stream = subscription.into_stream();

    println!("Subscribed to new Ethereum blocks");

    while let Some(block) = stream.next().await {
        println!(
            "New Ethereum block: {}",
            block.number
        );

        let full_block = http_provider
            .get_block_by_number(block.number.into())
            .full()
            .await?;
        
        if let Some(full_block) = full_block {
            println!(
              "Block hash: {:?}",
              full_block.header.hash
            );
            let block_timestamp = full_block.header.timestamp;

            if let Some(transactions) = full_block.transactions.as_transactions() {
                println!(
                  "Transactions: {}",
                  transactions.len()
                );
                println!(
                  "Block Timestamp: {}",
                  block_timestamp
                );

                for transaction in transactions {
                    let hash = transaction.tx_hash();
                    let from = transaction.from();
                    let to = transaction.to();
                    let value = transaction.value();

                    let normalized = shared::transaction::Transaction::new(
                        format!("{:?}", hash), 
                        format!("{:?}", from), 
                        to.map(|address| format!("{:?}", address)), 
                        value.to_string(), 
                        block_timestamp as i64
                    );

                    println!(" \nTX:");
                    println!("  Hash:  {}", normalized.hash);
                    println!("  From:  {}", normalized.from);
                    println!("  To:    {:?}", normalized.to);
                    println!("  Amount wei: {} wei", normalized.amount_wei);

                    tx.send(normalized).await?;
                }
            } else {
              println!("Block does not contain full transactions");
            }
        }
    }

    Ok(())
}