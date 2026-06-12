mod generator;
mod transaction;

use generator::Generator;
use std::time::Duration;
use tokio::time::sleep;
#[tokio::main]
async fn main() {
    println!("Blockchain Intelligence Platform");

    let mut generator = Generator::new();

    loop {
        let tx = generator.generate();
        
        tx.summary();

        sleep(Duration::from_secs(1)).await;
    }
}
