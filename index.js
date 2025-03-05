/**
 * Google Cloud Bucket name
 */
const BUCKET_NAME = "bucket-name";
/**
 * With minimal headers: id, gcloudPath, fileExtension
 * Just export the data from the db to a CSV file.
 * @example ```
 * id,gcloudPath,fileExtension
 * 1,images/1.jpg,jpg
 * 2,images/2.png,png
 * ```
 */
const INPUT_FILE = "./files.csv";
/**
 * The output file where the SQL queries will be written.
 */
const OUTPUT_FILE = "./result.txt";

const Papa = require("papaparse");
const fs = require("node:fs/promises");
const { loadImage } = require("canvas");
const { Storage } = require("@google-cloud/storage");

// Make sure you're logged in with `gcloud auth application-default login`
const storage = new Storage();

/**
 * @typedef {Object} FileInfo
 * @property {string} id - The unique identifier for the file.
 * @property {string} gcloudPath - The Google Cloud storage path for the file.
 * @property {string} fileExtension - The file's extension (e.g., '.jpg', '.png').
 */

/**
 * Read the input CSV file
 * @returns {Promise<FileInfo[]>}
 */
async function prepare() {
  const data = await fs.readFile(INPUT_FILE, "utf8");

  return new Promise((resolve, reject) => {
    Papa.parse(data, {
      skipEmptyLines: true,
      header: true,
      complete: (results) => resolve(results.data),
      error: (error) => reject(error),
    });
  });
}

/**
 * Download files from Google Cloud Storage.
 * @param {FileInfo[]} data
 * @returns {Promise<void>}
 */
async function download(data) {
  const total = data.length;
  for (let i = 0; i < total; i++) {
    const file = data[i];
    const destinationDest = `tmp/${file["id"]}.${file["fileExtension"]}`;

    await storage.bucket(BUCKET_NAME).file(file.gcloudPath).download({
      destination: destinationDest,
    });

    process.stdout.write(
      `\rProgress: ${i}/${total} (${((i / total) * 100).toFixed(2)}%)`
    );
  }

  process.stdout.write(`\rProgress: ${total}/${total} (100.00%)\n`);
}

/**
 * Generate SQL queries to update the metadata of the files.
 * @returns {Promise<string[]>}
 */
async function generateSQL() {
  const tmp = await fs.readdir("tmp");
  const files = tmp.map((file) => `tmp/${file}`);
  const results = [];

  for (let i = 1; i < files.length; i++) {
    const { width, height } = await loadImage(files[i]).catch(() => ({
      width: -1,
      height: -1,
    }));
    const id = files[i].split("/")[1].split(".")[0];

    if (id === undefined || id === "") {
      console.error("\nInvalid id:", files[i], "\n");
      continue;
    }

    results.push(
      `UPDATE "CustomFile" SET "metadata" = '{"width": ${width}, "height": ${height}}'::jsonb WHERE id = '${id}';\n`
    );

    process.stdout.write(
      `\rProgress: ${i}/${files.length} (${((i / files.length) * 100).toFixed(
        2
      )}%)`
    );

    // Write to the file every 200 lines to avoid memory issues
    if (results.length % 200 === 0) {
      await fs.appendFile(OUTPUT_FILE, results.join(""));
      results.length = 0;
    }
  }

  // Write the remaining lines
  await fs.appendFile(OUTPUT_FILE, results.join(""));

  process.stdout.write(
    `\rProgress: ${files.length}/${files.length} (100.00%)\n`
  );
  return files.length;
}

/**
 * @tutorial $ node index.js
 */
async function main() {
  console.log("Reading Input File...");
  const data = await prepare();

  console.log("Downloading Files from GC...");
  await download(data);

  console.log("Generating SQL...");
  const expectedCount = await generateSQL();

  console.log("Checking the Line Count...");
  const actualCount = (await fs.readFile(OUTPUT_FILE, "utf8")).split(
    "\n"
  ).length;
  console.log(
    `Total lines: ${actualCount - 1}. Should be: ${expectedCount - 1}.`
  );

  console.log("Done!");
}

main().catch(console.error);
