const path = require("node:path");
const fs = require("node:fs");
const readline = require('node:readline');
// const {parse} = require("../scripts/parse");
// read file from folder
// change this to the path that contains mapReduce results
const assert = require("node:assert");

const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;
const { exec } = require('child_process');



function index(foldername, stopWordsPath) {
    const path = require("node:path");
    const fs = require("node:fs");
    // Util functions
    function createFileIfNotExists(filePath, content = '') {
        if (!fs.existsSync(filePath)) {
            try {
                fs.writeFileSync(filePath, content);
                console.log(`File ${filePath} created successfully.`);
            } catch (err) {
                console.error(`Error creating file ${filePath}:`, err);
            }
        } else {
            console.log(`File ${filePath} already exists.`);
        }
    }
    
    function updateLocalIndex(localIndexPath, newIndex) {
        try {
            const data = fs.readFileSync(localIndexPath, 'utf8');
            const globalIndex = JSON.parse(data);
    
            // Merge the existing data with the new data
            Object.keys(newIndex).forEach(token => {
                if (token in globalIndex) {
                    // If key exists in both indexes, merge counts
                    globalIndex[token].push(newIndex[token]);
                } else {
                    // If key exists only in index1, add it to merged index
                    globalIndex[token] = [newIndex[token]];
                }
            });
    
            // Write the updated index back to the file
            fs.writeFileSync(localIndexPath, JSON.stringify(globalIndex, null, 2));
        } catch (err) {
            console.error('Error reading JSON file:', err);
        }
    }

    function parse(rawData, stopWords) {
        try {
            const { compile } = require('html-to-text');
            const natural = require('natural');
            const path = require("node:path");
            const fs = require("node:fs");
            
            // retrieve tokens
            const tokenizer = new natural.WordTokenizer();
            var tokens = tokenizer.tokenize(rawData);
        
            // removing stop words
            tokens = tokens.filter(token => !stopWords.has(token));
        
            // stemming
            tokens = tokens.map(token => natural.PorterStemmer.stem(token));

            return tokens
        } catch (err) {
            console.error(err);
            return []
        }
    }

    distribution.local.store.resolveFilePath(`crawl`, null, (_, folderPath) => {
        if (!fs.existsSync(path.join(folderPath, `${foldername}`))) {
            console.log(`${folderPath/foldername} doesn't exist, skip`)
            return
        }
        // Create batch index folder
        const indexDirPath = path.join(folderPath, `index-${foldername}`)
        if (!fs.existsSync(indexDirPath)) {
            fs.mkdirSync(indexDirPath, { recursive: true });
        }

        // Create file to store index if it doesn't exist
        const localIndexPath = path.join(folderPath, `index-${foldername}/index.json`)
        createFileIfNotExists(localIndexPath, '{}');

        // Load stopwords into a map
        const stopWords = new Set(fs.readFileSync(stopWordsPath, 'utf8').split('\n').map(word => word.trim()));
        
        const dirKey = {key: {folder: foldername, key: null}, gid: "crawl"};
        distribution.local.store.getPromise(dirKey).then((keys) => {
            if (keys.length === 0) {
                console.log("No crawled results found.");
                return;
            }
            let counter = 1;
            let startTime = Date.now();
            keys.forEach((key) => {
                const fileKey = {key: {folder: foldername, key: key}, gid: "crawl"};
                distribution.local.store.getPromise(fileKey).then((crawlRes) => {
                    const url = Object.keys(crawlRes)[0];
                    const rawData = crawlRes[url];
                    const parsedTokens = parse(rawData, stopWords);
                    // index for one html doc
                    var fileIndex = {};
                    parsedTokens.forEach(token => {
                        if (!(token in fileIndex)) {
                            fileIndex[token] = { count: 1, url: url };
                        } else {
                            fileIndex[token].count++;
                        }
                    });

                    // Store to the global index
                    updateLocalIndex(localIndexPath, fileIndex);

                    if (counter == keys.length) {
                        let endTime = Date.now();
                        let duration = endTime - startTime;
                        console.log(`Finished Indexing ${keys.length} websites. Took ${duration}ms`)
                    }
                    counter += 1;
                });
            });

            console.log(`Starting Indexing ${keys.length} websites.`)
        });
    })
    
}


// const n1 = { ip: "127.0.0.1", port: 7110 };
// const n2 = { ip: "127.0.0.1", port: 7111 };
// const n3 = { ip: "127.0.0.1", port: 7112 };

// const crawlGroup = {
//   [id.getSID(n1)]: n1,
//   [id.getSID(n2)]: n2,
//   [id.getSID(n3)]: n3,
// };


// lsof -ti:7112 | xargs kill; lsof -ti:7111 | xargs kill; lsof -ti:7110
function startNodes() {
  return Promise.all(
    Object.values(crawlGroup).map((n) =>
      distribution.local.status.spawnPromise(n),
    ),
  );
}

// Pre-define paths for internal storage
const stopWordsPath = path.join(__dirname, "../data/stopwords.txt");

// Define service
const boogleService = {};
boogleService.index = index;


let localServer = null;
// distribution.node.start((server) => {
//     localServer = server;
//     const crawlConfig = { gid: "crawl" };
//     startNodes().then(() => {
//       groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
//         assert(Object.values(e).length === 0);
//         // var key = {folder: "map-crawler", key: null};
//         // distribution.crawl.store.getPromise(key).then((keys) => {
//         //     console.log(keys)
//         // });
//         distribution.crawl.routes.put(boogleService, 'boogleService', (e, v) => {
//             assert(Object.values(e).length === 0);
//             const remote = {service: 'boogleService', method: 'index'};
//             distribution.crawl.comm.send([localIndexPath, stopWordsPath], remote, (e, v) => {
//               console.log(e)
//               assert(Object.values(e).length === 0);
//               console.log("COMPLETE INDEXING=========================");
//             });
//         });
//       });
//     });
//   });

module.exports = { index };

