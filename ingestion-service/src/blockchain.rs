use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::network::TransactionResponse;
use futures_util::StreamExt;

pub async fn subscribe_blocks(
    ws_url: &str,
    http_url: &str
) -> Result<(), Box<dyn std::error::Error>> {
    // Websocket provider
    let ws = WsConnect::new(ws_url);

    let ws_provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await?;

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

            if let Some(transactions) = full_block.transactions.as_transactions() {
                println!(
                  "Transactions: {}",
                  transactions.len()
                );

                for transaction in transactions {

                    println!(
                        "  TX: {:?}",
                        transaction.tx_hash()
                    );
                }
            } else {
              println!("Block does not contain full transactions");
            }
        }
    }

    Ok(())
}