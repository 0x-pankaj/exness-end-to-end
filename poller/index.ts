import WebSocket from 'ws';
import redis from "redis";

const ws = new WebSocket('wss://ws.backpack.exchange/');


const redisClient = redis.createClient();

const publiser = redisClient.duplicate();
await publiser.connect();
console.log('Connected to Redis');

ws.on('open', function open() {
  console.log('WebSocket connection opened');
  ws.send(
    JSON.stringify({ "method": "SUBSCRIBE", "params": ["bookTicker.SOL_USDC", "bookTicker.BTC_USDC",] }),
  );
});

// const latest_price: Map<string, any> = new Map();




ws.on('message', async function message(data) {
  // console.log('Received data:', data);
  const trade = JSON.parse(data.toString());
  console.log("Price from websocket: ", trade);
  // console.log(trade);
  //   if (trade.e === 'trade') {
  //     console.log(`${trade.s} ${trade.p} ${trade.q}`);
  //   }

  // const latest_price =  {
  //     symbol: trade.s,
  //     price: trade.p,
  //     quantity: trade.q,
  //     timestamp: trade.T
  // }

  // type latest_price = Record<string, any>

  // latest_price.set(trade.data.s,
  //   { symbol: trade.data.s, buyPrice: Math.round((Number(trade.data.b) * 1.01 * Math.pow(10, 4))), sellPrice: Math.round((Number(trade.data.a) * 0.99 * Math.pow(10, 4))), decimals: 4 }
  // );

  // await publiser.publish('price_updates', JSON.stringify(Array.from(latest_price.values())));


  //   data: {
  //     A: "16.19",
  //     B: "10.52",
  //     E: 1769861944213313,
  //     T: 1769861944209551,
  //     a: "115.52",
  //     b: "115.51",
  //     e: "bookTicker",
  //     s: "SOL_USDC",
  //     u: 3440905105,
  //   },
  //   stream: "bookTicker.SOL_USDC",
  // }

  const DECIMAL = 4;

  const priceObject = {
    symbol: trade.data.s,
    buyPrice: (Number(trade.data.a) * 1.01 * Math.pow(10, DECIMAL)).toFixed(0),
    sellPrice: (Number(trade.data.b) * 0.99 * Math.pow(10, DECIMAL)).toFixed(0),
    decimals: DECIMAL, // Changed from decimal to decimals to match engine expectation
    action: "LATEST_PRICE"
  }

  await publiser.publish("price_updates", JSON.stringify(priceObject));

  // Push to orders stream for the engine
  try {
    await publiser.xAdd("orders", "*", { data: JSON.stringify(priceObject) });
  } catch (err) {
    console.error("Failed to push to stream:", err);
  }




});

// setInterval(async () => {

// console.log('Latest prices map:', Object.fromEntries(latest_price));
// console.log("price updates: ", Array.from(latest_price));
// console.log(latest_price)
// }, 100);


ws.on('error', function error(err) {
  console.error('WebSocket error:', err);
});

