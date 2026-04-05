import dotenv from "dotenv";
import path from "node:path";
import { existsSync } from "node:fs";
import cors from "cors";
import express from "express";
import researchRouter from "./routes/research";

dotenv.config();
const app = express();
const port = Number(process.env.PORT ?? "3000");

app.use(cors());
app.use(express.json());
app.use(researchRouter);

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true });
});

const frontendDist = path.join(process.cwd(), "frontend", "dist");
if (existsSync(frontendDist)) {
  app.use(express.static(frontendDist));
  app.get(/.*/, (_req, res) => res.sendFile(path.join(frontendDist, "index.html")));
}

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
  console.log("Use POST /research and poll GET /status/:jobId.");
});
