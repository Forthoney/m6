const { compile } = require('html-to-text');
const natural = require('natural');
const path = require("node:path");
const fs = require("node:fs");
const readline = require('node:readline');
const crypto = require('crypto');

const folderPath = path.join(__dirname, "../test/crawled");
const parsedOutPath = path.join(__dirname, "../test/parsed");

const stopWordsPath = path.join(__dirname, "../scripts/stopwords.txt");
// Store stopwords in a set so the lookup is more efficient
const stopWords = new Set(fs.readFileSync(stopWordsPath, 'utf8').split('\n').map(word => word.trim()));

/**
 * The ID is the SHA256 hash of the JSON representation of the object
 * @param {any} obj
 * @return {ID}
 */
function getID(url) {
  const hash = crypto.createHash('sha256');
  hash.update(url);
  return hash.digest('hex');
}

function parse() {
  try {
    console.log('hi')
    const files = fs.readdirSync(folderPath);
    files.forEach(file => {
        const filePath = path.join(folderPath, file);
        var url;
        // read url from 1st line
        const rl = readline.createInterface({
          input: fs.createReadStream(filePath),
          output: process.stdout,
          terminal: false
        });
        rl.once('line', (line) => {
          url = line;
          rl.close(); // Close the readline interface after reading the first line

          // url is also parsed as tokens but it's fine.
          const rawData = fs.readFileSync(filePath, 'utf8');

          // retrieve tokens
          const tokenizer = new natural.WordTokenizer();
          var tokens = tokenizer.tokenize(rawData);
          // stemming
          tokens = tokens.map(token => natural.PorterStemmer.stem(token));

          // removing stop words
          tokens = tokens.filter(token => !stopWords.has(token));

          // write to parsed file
          const parsedFilePath = path.join(parsedOutPath, getID(url) + 'txt');
          fs.writeFileSync(parsedFilePath, `${url}\n` + tokens.join('\n'));
        });
    });
} catch (err) {
    console.error('Error reading folder:', err);
}
}


// const files = fs.readdirSync(folderPath);
// var obj = {}
// files.forEach(file => {
//     const filePath = path.join(folderPath, file);
//     const fileContent = fs.readFileSync(filePath, 'utf8');
//     const url = "url here"
//     obj[url] = fileContent;
// });
// const inputPath = path.join(__dirname, "../test/crawled/1.json");
// fs.writeFileSync(inputPath, JSON.stringify(obj, null, 2));

parse()
