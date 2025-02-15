import 'dotenv/config';
import fs from "fs";
import csv from "csv-parser";
import { createObjectCsvWriter as createCsvWriter } from "csv-writer";
import OpenAI from "openai";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const INPUT_CSV = "code_block1000000-2000000.csv";
const OUTPUT_CSV = "classify_block1000000-2000000.csv";
const MODEL_NAME = "gpt-4o";
const REQUESTS_PER_SECOND = 1;
const SKIP_PROCESSED = true;
const MIN_CODE_LENGTH = 20;

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

async function getArtisticScoreAndReason(codeSnippet) {
  const systemPrompt = `
あなたはSolidityコードがどれほど芸術的かを1〜3の整数で評価し、その理由を簡潔に説明するアシスタントです。
- 1: 実用的なコード (一般的なスマートコントラクト)
- 2: 実用的だが、詩的な要素を含むコード
- 3: 主要な目的が芸術的表現であるコード

【出力フォーマット】
スコアと理由を以下の形式で出力してください:
1 | Solidityの実用的な構造を持つ標準的なスマートコントラクト。

理由は100文字以内とし、シンプルな説明にしてください。
`.trim();

  const userPrompt = `
以下のSolidityコードの芸術度 (1-3) を評価し、その理由を述べてください。
必ず以下の形式で出力してください：
<スコア> | <理由>

\`\`\`
${codeSnippet}
\`\`\`
`.trim();

  try {
    const response = await openai.chat.completions.create({
      model: MODEL_NAME,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      temperature: 0.0,
    });

    const content = response.choices[0].message.content.trim();
    const parts = content.split("|").map((part) => part.trim());

    if (parts.length !== 2) {
      console.warn("Unexpected API response format:", content);
      return { score: null, reason: "" };
    }

    const score = parseInt(parts[0], 10);
    const reason = parts[1];

    if (isNaN(score) || score < 1 || score > 3) {
      console.warn("Invalid score received:", score);
      return { score: null, reason: "" };
    }

    return { score, reason };
  } catch (err) {
    console.error("OpenAI API error:", err.message);
    return { score: null, reason: "" };
  }
}

async function main() {
  let processedAddresses = new Set();
  if (SKIP_PROCESSED && fs.existsSync(OUTPUT_CSV)) {
    const lines = fs.readFileSync(OUTPUT_CSV, "utf8").split("\n");
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      const cols = line.split(",");
      const address = cols[1];
      if (address) {
        processedAddresses.add(address);
      }
    }
  }

  const processedCodes = {};

  const csvWriter = createCsvWriter({
    path: OUTPUT_CSV,
    header: [
      { id: "block_number", title: "block_number" },
      { id: "address", title: "address" },
      { id: "is_erc20", title: "is_erc20" },
      { id: "is_erc721", title: "is_erc721" },
      { id: "code", title: "code" },
      { id: "artistic_score", title: "artistic_score" },
      { id: "artistic_reason", title: "artistic_reason" },
      { id: "duplicate_of_address", title: "duplicate_of_address" },
    ],
    append: fs.existsSync(OUTPUT_CSV),
  });

  const intervalMs = 1000 / REQUESTS_PER_SECOND;
  let lastRequestTime = 0;

  const records = [];
  fs.createReadStream(INPUT_CSV)
    .pipe(csv())
    .on("data", (row) => {
      records.push(row);
    })
    .on("end", async () => {
      console.log(`CSV loaded: ${records.length} records`);

      let count = 0;
      for (const row of records) {
        const address = row.address;
        const code = row.code;

        if (SKIP_PROCESSED && processedAddresses.has(address)) {
          console.log(`[SKIP-ADDR] already processed address: ${address}`);
          continue;
        }

        if (code.length <= MIN_CODE_LENGTH) {
          console.log(`[SKIP-SHORT] code is too short: ${address} (length=${code.length})`);
          await csvWriter.writeRecords([
            {
              block_number: row.block_number,
              address: address,
              is_erc20: row.is_erc20,
              is_erc721: row.is_erc721,
              code: "",
              artistic_score: "",
              artistic_reason: "",
              duplicate_of_address: "TOO_SHORT",
            },
          ]);
          continue;
        }

        if (processedCodes[code]) {
          const originalAddr = processedCodes[code];
          console.log(`[SKIP-CODE] duplicate code: ${address} (original=${originalAddr})`);
          await csvWriter.writeRecords([
            {
              block_number: row.block_number,
              address: address,
              is_erc20: row.is_erc20,
              is_erc721: row.is_erc721,
              code: "",
              artistic_score: "",
              artistic_reason: "",
              duplicate_of_address: originalAddr,
            },
          ]);
          continue;
        }

        const now = Date.now();
        const elapsed = now - lastRequestTime;
        if (elapsed < intervalMs) {
          await new Promise((resolve) => setTimeout(resolve, intervalMs - elapsed));
        }

        console.log(`[processing] address: ${address}`);
        const { score, reason } = await getArtisticScoreAndReason(code);
        lastRequestTime = Date.now();

        await csvWriter.writeRecords([
          {
            block_number: row.block_number,
            address: row.address,
            is_erc20: row.is_erc20,
            is_erc721: row.is_erc721,
            code: code.replace(/\r?\n/g, "\\n"),
            artistic_score: score === null ? "" : score,
            artistic_reason: reason,
            duplicate_of_address: "",
          },
        ]);

        processedAddresses.add(address);
        processedCodes[code] = address;
        count++;
      }

      console.log(`\n=== completed: ${count} records ===`);
    });
}

main().catch((err) => {
  console.error("Fatal error:", err);
});
