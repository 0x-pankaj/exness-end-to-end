use tungstenite::{Message, connect};

// currentPrice =   [
//     {"symbol":"BTC_USDC_PERP","buyPrice":1125166260,"sellPrice":1102886730,"decimals":4},
//     {"symbol":"BTC_USDC","buyPrice":1125731860,"sellPrice":1103441130,"decimals":4},
//     {"symbol":"SOL_USDC_PERP","buyPrice":2119586,"sellPrice":2077713,"decimals":4},
//     {"symbol":"SOL_USDC","buyPrice":2120293,"sellPrice":2078406,"decimals":4}
// ]

fn main() {
    let (mut socket, response) = connect("ws://localhost:8080").expect("Can't connect");

    // println!("Response HTTP code: {}", response.status());
    // println!("Response contains the following headers:");

    // socket.send(Message::Text("WebSocket".into())).unwrap();
    loop {
        let msg = socket.read().expect("Error reading message");
        println!("currentPrice: {msg}");
    }
    // socket.close(None);
}

//TODO
// listening to the webocket for latest price list
// get price destructure it to the struct of price
