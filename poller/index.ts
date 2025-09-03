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
    JSON.stringify({"method":"SUBSCRIBE","params":["bookTicker.SOL_USDC","bookTicker.SOL_USDC_PERP", "bookTicker.BTC_USDC","bookTicker.BTC_USDC_PERP"]}),
  );
});

const latest_price: Map<string, any> = new Map();




ws.on('message', function message(data) {
    // console.log('Received data:', data);
  const trade = JSON.parse(data.toString());
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

    latest_price.set(trade.data.s,
        {symbol: trade.data.s , buyPrice: Math.round((Number(trade.data.b) * 1.01 * Math.pow(10, 4))  ), sellPrice: Math.round((Number(trade.data.a) * 0.99 * Math.pow(10, 4))), decimals: 4}
    );

    


    // publiser.lPush('trades_list', JSON.stringify(trade));
    
    
  });
  
  setInterval(async() => {
    
    // console.log('Latest prices map:', Object.fromEntries(latest_price));
    // console.log("price updates: ", Array.from(latest_price));
    await publiser.publish('price_updates', JSON.stringify(Array.from(latest_price.values())));
    // console.log(latest_price)
  }, 100);


ws.on('error', function error(err) {
    console.error('WebSocket error:', err);
});

