import 'dotenv/config';
import fs from "fs";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

const INPUT_CSV = "data/addresses_block1-1000000.csv";
const OUTPUT_CSV = "data/code_block1-1000000.csv";

const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;
const ETHERSCAN_URL = "https://api.etherscan.io/api";
const REQUESTS_PER_SECOND = 3;
const SKIP_PROCESSED = true;

async function getProcessedAddresses(outputCsvPath) {
  return new Promise((resolve) => {
    let processed = new Set();
    if (!fs.existsSync(outputCsvPath)) {
      return resolve(processed);
    }
    fs.createReadStream(outputCsvPath)
      .pipe(csv())
      .on("data", (row) => {
        processed.add(row.address);
      })
      .on("end", () => {
        resolve(processed);
      });
  });
}

async function fetchContractSource(address) {
  const params = new URLSearchParams({
    module: "contract",
    action: "getsourcecode",
    address: address,
    apikey: ETHERSCAN_API_KEY,
  });

  while (true) {
    try {
      const response = await fetch(`${ETHERSCAN_URL}?${params}`);
      if (!response.ok) {
        console.log(`HTTP error: ${response.status}. Retrying in 3 sec...`);
        await new Promise((resolve) => setTimeout(resolve, 3000));
        continue;
      }

      const data = await response.json();

      if (data.message === "NOTOK") {
        console.log(`Rate limit reached or error: ${data.result}`);
        console.log("Waiting 3 seconds before retry...");
        await new Promise((resolve) => setTimeout(resolve, 3000));
        continue;
      }

      if (!Array.isArray(data.result) || data.result.length === 0) {
        console.log(`Unexpected result format for ${address}:`, data);
        return null;
      }

      const contractInfo = data.result[0];
      const sourceCode = contractInfo.SourceCode?.trim();
      return sourceCode && sourceCode !== "0x" ? sourceCode : null;
    } catch (error) {
      console.error(`Network error: ${error.message}. Retrying in 10 sec...`);
      await new Promise((resolve) => setTimeout(resolve, 10000));
    }
  }
}

async function createCsvWriter(outputPath) {
  const headers = [
    { id: "block_number", title: "block_number" },
    { id: "address", title: "address" },
    { id: "is_erc20", title: "is_erc20" },
    { id: "is_erc721", title: "is_erc721" },
    { id: "code", title: "code" },
  ];

  const fileExists = fs.existsSync(outputPath);
  if (!fileExists) {
    fs.writeFileSync(outputPath, headers.map(h => h.title).join(",") + "\n");
  }

  return createObjectCsvWriter({
    path: outputPath,
    header: headers,
    append: true,
  });
}

async function main() {
  const processedAddresses = SKIP_PROCESSED ? await getProcessedAddresses(OUTPUT_CSV) : new Set();
  const csvWriter = await createCsvWriter(OUTPUT_CSV);

  let records = [];
  fs.createReadStream(INPUT_CSV)
    .pipe(csv())
    .on("data", (row) => {
      records.push(row);
    })
    .on("end", async () => {
      console.log(`Loaded ${records.length} records. Starting processing...`);

      let callCount = 0;
      let startTime = Date.now();

      for (const row of records) {
        const { block_number, address, is_erc20, is_erc721 } = row;

        if (SKIP_PROCESSED && processedAddresses.has(address)) {
          console.log(`[SKIP] Already processed: ${address}`);
          continue;
        }

        callCount++;
        if (callCount > REQUESTS_PER_SECOND) {
          const elapsed = Date.now() - startTime;
          if (elapsed < 1000) {
            await new Promise((resolve) => setTimeout(resolve, 1000 - elapsed));
          }
          startTime = Date.now();
          callCount = 1;
        }

        console.log(`[FETCH] Fetching contract: ${address}`);
        const sourceCode = await fetchContractSource(address);

        if (sourceCode) {
          await csvWriter.writeRecords([
            {
              block_number,
              address,
              is_erc20,
              is_erc721,
              code: sourceCode.replace(/\r?\n/g, "\\n"),
            },
          ]);
          processedAddresses.add(address);
          console.log(`[OK] Saved: ${address}`);
        } else {
          console.log(`[NG] No contract: ${address}`);
        }
      }

      console.log("=== All done ===");
    });
}

main().catch((err) => {
  console.error("Fatal error:", err);
});
