use rinha_native::IndexVariant;

fn main() {
    match IndexVariant::open("/tmp/native_ivf.idx") {
        Ok(_) => println!("Success!"),
        Err(e) => println!("Error: {}", e),
    }
}
