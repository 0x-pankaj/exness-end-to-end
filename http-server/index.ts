import { randomUUIDv7 } from "bun";
import cors from "cors";
import Express from "express";
import redis from "redis";
import router from "./routes/route";

const app = Express();

app.use(cors());
app.use(Express.json());

// {
// 	asset: "BTC",
// 	type: "long" | "short",
// 	margin: 50000, // decimal is 2, so this means 500$
// 	leverage: 10, // so the user is trying to buy $5000 of exposure
// 	slippage: 100, // in bips, so this means 1%
// }


app.use("/api", router);


app.listen(5000, () => {
  console.log("Server is running on port 5000");
});
