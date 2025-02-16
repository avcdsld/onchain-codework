import fs from 'fs';
import csv from 'csv-parser';

async function countNonErc20(csvFilePath) {
    let count = 0;

    return new Promise((resolve, reject) => {
        fs.createReadStream(csvFilePath)
            .pipe(csv())
            .on('data', (row) => {
                if (String(row.is_erc20).toLowerCase() !== 'true') {
                    count++;
                }
            })
            .on('end', () => {
                console.log(`Non-ERC20 count: ${count}`);
                resolve(count);
            })
            .on('error', (error) => {
                console.error('Error reading the CSV file:', error);
                reject(error);
            });
    });
}

// コマンドライン引数から CSV ファイルパスを取得
const csvFilePath = process.argv[2];
if (!csvFilePath) {
    console.error('Usage: node countNonErc20.mjs <csv-file-path>');
    process.exit(1);
}

countNonErc20(csvFilePath);
