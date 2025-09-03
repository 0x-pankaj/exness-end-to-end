import { randomUUIDv7 } from "bun";
import cors from "cors";
import Express from "express";
import redis from "redis";

const app = Express();

app.use(cors());
app.use(Express.json());
const redisClient = redis.createClient();
const queue = redisClient.duplicate();
await queue.connect();
const subscriber = redisClient.duplicate();
await subscriber.connect();

// {
// 	asset: "BTC",
// 	type: "long" | "short",
// 	margin: 50000, // decimal is 2, so this means 500$
// 	leverage: 10, // so the user is trying to buy $5000 of exposure
// 	slippage: 100, // in bips, so this means 1%
// }

app.post("/api/v1/trade/create", async (req, res) => {
  try {
    const { asset, type, margin, leverage, slippage } = req.body;

    if (!asset || !type || !margin || !leverage || !slippage) {
      return res.status(400).json({ error: "All fields are required" });
    }

    if (leverage < 1 || leverage > 100) {
      return res
        .status(400)
        .json({ error: "Leverage must be between 1 and 100" });
    }

    if (margin <= 0) {
      return res.status(400).json({ error: "Margin must be positive" });
    }

    if (slippage < 0 || slippage > 200) {
      return res
        .status(400)
        .json({ error: "Slippage must be between 0 and 100" });
    }

    const orderId = randomUUIDv7();
    const order = {
      data: {
        action: "CREATE_ORDER",
        user: req.userId,
        orderId,
        asset,
        type,
        margin,
        leverage,
        slippage,
      },
    };

    const response = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        subscriber.unsubscribe(orderId);
        reject(new Error("Order processing timeout"));
      }, 30000);

      try {
        subscriber.subscribe(orderId, (msg) => {
          clearTimeout(timeout);
          subscriber.unsubscribe(orderId);

          try {
            const parsedMsg = JSON.parse(msg);
            resolve(parsedMsg);
          } catch (parseError) {
            reject(new Error("Invalid response format"));
          }
        });

        queue.lPush("orders", JSON.stringify(order));
        //stream redis
        queue.xAdd("orders", "*", order.data);
      } catch (error) {
        clearTimeout(timeout);
        subscriber.unsubscribe(orderId);
        reject(error);
      }
    });

    console.log("Engine response:", response);

    /*
        response = {
            action: "ORDER_SUCCESS" | "ORDER_FAILED",
            data: {
                orderId: string,
                message?: string
            }
        }
        */

    if (response.action === "ORDER_FAILED") {
      return res.status(400).json({
        error: "Order failed",
        orderId: response.data.orderId,
        message: response.data.message || "Unknown error",
      });
    }

    if (response.action === "ORDER_SUCCESS") {
      return res.status(201).json({
        orderId: response.data.orderId,
        message: "Order created successfully",
      });
    }

    return res.status(500).json({
      error: "Unexpected response from trading engine",
    });
  } catch (error) {
    console.error("Trade creation error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/v1/trade/close", async (req, res) => {
  try {
    const { orderId } = req.body;

    if (!orderId) {
      return res.status(400).json({ error: "Order ID is required" });
    }

    const order = {
      action: "CLOSE_ORDER",
      orderId,
      user: req.userId,
    };

    /*
        action: "ORDER_SUCCESS" | "ORDER_FAILED",
        message?: string


        */

    const response = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        subscriber.unsubscribe(orderId);
        clearTimeout(timeout);
        reject(new Error("Order processing timeout"));
      }, 20000);

      try {
        subscriber.subscribe(orderId, (msg) => {
          clearTimeout(timeout);
          subscriber.unsubscribe(orderId);

          const parsedMsg = JSON.parse(msg);
          resolve(parsedMsg);
        });

        // queue.lPush("orders", JSON.stringify(order));
        queue.XADD("orders", "*", order);
      } catch (error) {
        clearTimeout(timeout);
        subscriber.unsubscribe(orderId);
        reject(error);
      }
    });

    console.log("Engine response:", response);

    /*
        response = {
            action: "ORDER_SUCCESS" | "ORDER_FAILED",
            data: {
                orderId: string,
                message?: string
            }
        }
        */

    if (response.action === "ORDER_FAILED") {
      return res.status(400).json({
        message: response.data.message || "Unknown error",
      });
    }

    res.status(200).json({
      message: "Order closed successfully",
    });
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("api/v1/balance/usd", async (req, res) => {
  try {
    const order = {
      action: "GET_BALANCE_USD",
      orderId: randomUUIDv7(),
      user: req.userId,
    };

    const response = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        subscriber.unsubscribe(order.orderId);
        clearTimeout(timeout);
        reject(new Error("Order processing timeout"));
      }, 20000);

      try {
        subscriber.subscribe(order.orderId, (msg) => {
          clearTimeout(timeout);
          subscriber.unsubscribe(order.orderId);
          const parsedMsg = JSON.parse(msg);
          resolve(parsedMsg);
        });

        // queue.lPush("orders", JSON.stringify(order));
        queue.XADD("orders", "*", order);
      } catch (error) {
        clearTimeout(timeout);
        subscriber.unsubscribe(order.orderId);
        reject(error);
      }
    });

    console.log("Engine response:", response);

    /*
        response = {
            action: "BALANCE_USD",
            data: {
                balance: number
            }
        }
        */

    if (
      response.action !== "BALANCE_USD" ||
      typeof response.data.balance !== "number"
    ) {
      return res.status(500).json({ error: "Unexpected response from engine" });
    }

    res.status(200).json({ balance: response.data.balance });
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/v1/balance", async (req, res) => {
  try {
    const order = {
      action: "GET_BALANCE",
      orderId: randomUUIDv7(),
      user: req.userId,
    };

    const response = await new Promise((res, rej) => {
      const timeout = setTimeout(() => {
        subscriber.unsubscribe(order.orderId);
        clearTimeout(timeout);
        rej(new Error("Order processing timeout"));
      }, 20000);

      try {
        subscriber.subscribe(order.orderId, (msg) => {
          clearTimeout(timeout);
          subscriber.unsubscribe(order.orderId);
          const parsedMsg = JSON.parse(msg);
          res(parsedMsg);
        });

        // queue.lPush("orders", JSON.stringify(order));
        queue.XADD("orders", "*", order);
      } catch (error) {
        clearTimeout(timeout);
        subscriber.unsubscribe(order.orderId);
        rej(error);
      }
    });

    console.log("Engine response:", response);

    /*

        resonse we get = {
            action: "BALANCE",
            "BTC": {
                balance: number,
                decimals: number
            },
            "ETH": {
                balance: number,
                decimals: number
            }
        }

            response we need to send  = {
	        "BTC": {
		    "balance": 10000000,
		    "decimals": 4
	            }
            }
                map => object.fromEntries(map)
        */

    if (response.action !== "BALANCE") {
      return res.status(500).json({ error: "Unexpected response from engine" });
    }

    res.status(200).json({ balance: response });
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/v1/supportedAssets", async (req, res) => {
  try {
    const order = {
      action: "GET_SUPPORTED_ASSETS",
      orderId: randomUUIDv7(),
      user: req.userId,
    };

    const response = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        subscriber.unsubscribe(order.orderId);
        clearTimeout(timeout);
        reject(new Error("Order processing timeout"));
      }, 20000);

      try {
        subscriber.subscribe(order.orderId, (msg) => {
          clearTimeout(timeout);
          subscriber.unsubscribe(order.orderId);
          const parsedMsg = JSON.parse(msg);
          resolve(parsedMsg);
        });

        // queue.lPush("orders", JSON.stringify(order));
        queue.XADD("orders", "*", order);
      } catch (error) {
        clearTimeout(timeout);
        subscriber.unsubscribe(order.orderId);
        reject(error);
      }
    });

    console.log("Engine response:", response);

    /*

        response = {
            action: "SUPPORTED_ASSETS",
            assets: [{
                symbol: "BTC",
                name: "bitcoin",
                imageUrl: "Image.com/png"
            },
            {
                symbol: "ETH",
                name: "ethereum",
                imageUrl: "Image.com/png"
            }
            ]
        }


     we need to send the same response to the client = {
	   "assets": [{
		symbol: "BTC",
		name: "Bitcoin",
		imageUrl: "image.com/png"
	}]
}
        */

    if (
      response.action !== "SUPPORTED_ASSETS" ||
      !Array.isArray(response.assets)
    ) {
      return res.status(500).json({ error: "Unexpected response from engine" });
    }

    res.status(200).json({ assets: response.assets });
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
});

// comment

app.listen(3000, () => {
  console.log("Server is running on port 3000");
});
