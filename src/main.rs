mod generator;
mod transaction;

use generator::Generator;
use std::thread;
use std::time::Duration;
fn main() {
    println!("Blockchain Intelligence Platform");

    let mut generator = Generator::new();

    loop {
        let tx = generator.generate();
        
        tx.summary();

        thread::sleep(Duration::from_secs(1));
    }
}
