import dotenv from "dotenv";
import express from "express";
import prospectsRouter from "./routes/prospects";

dotenv.config();
const app = express();
const port = Number(process.env.PORT ?? "3000");

app.use(express.json());
app.use("/api/v1/prospects", prospectsRouter);

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true });
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
  console.log("Use POST /api/v1/prospects/search in Postman.");
});
