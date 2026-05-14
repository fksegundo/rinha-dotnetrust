use rinha_native::NativeIndex;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::thread;

const PACKED_DIMS: usize = 16;
const REQUEST_BYTES: usize = PACKED_DIMS * size_of::<i16>();

fn main() {
    let index_path = std::env::var("RINHA_NATIVE_INDEX_PATH")
        .unwrap_or_else(|_| "/app/index/native.idx".to_string());
    let socket_path = std::env::var("RINHA_SEARCH_SOCKET")
        .unwrap_or_else(|_| "/sockets/search.sock".to_string());

    let index = Arc::new(
        NativeIndex::open(&index_path)
            .unwrap_or_else(|err| panic!("failed to open native index '{}': {}", index_path, err)),
    );

    let _ = fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)
        .unwrap_or_else(|err| panic!("failed to bind search socket '{}': {}", socket_path, err));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let index = Arc::clone(&index);
                thread::spawn(move || handle_client(stream, index));
            }
            Err(err) => eprintln!("[search-service] accept failed: {err}"),
        }
    }
}

fn handle_client(mut stream: UnixStream, index: Arc<NativeIndex>) {
    let mut request = [0u8; REQUEST_BYTES];
    loop {
        if stream.read_exact(&mut request).is_err() {
            return;
        }

        let mut query = [0i16; PACKED_DIMS];
        for (idx, chunk) in request.chunks_exact(2).enumerate() {
            query[idx] = i16::from_le_bytes([chunk[0], chunk[1]]);
        }

        let count = index.predict(&query);
        let response = [count.clamp(0, 5) as u8];
        if stream.write_all(&response).is_err() {
            return;
        }
    }
}
