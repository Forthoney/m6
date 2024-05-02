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



function index(prefixname, stopWordsPath) {
    const path = require("node:path");
    const fs = require("node:fs");
    // Util functions
    function createFileIfNotExists(filePath, content = '') {
        if (!fs.existsSync(filePath)) {
            try {
                fs.writeFileSync(filePath, content);
                // console.log(`File ${filePath} created successfully.`);
            } catch (err) {
                console.error(`Error creating file ${filePath}:`, err);
            }
        } else {
            // console.log(`File ${filePath} already exists.`);
        }
    }
    
    function updateIndex(indexPath, newIndex) {
        try {
            const data = fs.readFileSync(indexPath, 'utf8');
            const index = JSON.parse(data);
    
            // Merge the existing data with the new data
            Object.keys(newIndex).forEach(token => {
                if (token in index && !isNaN(index[token])) {
                    try {
                        // If key exists in both indexes, merge counts
                        index[token].push(newIndex[token]);
                    } catch (err) {
                        console.error('Error updating index:', err);
                    }
                } else {
                    // If key exists only in index1, add it to merged index
                    index[token] = [newIndex[token]];
                }
            });
    
            // Write the updated index back to the file
            fs.writeFileSync(indexPath, JSON.stringify(index, null, 2));
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
        const items = fs.readdirSync(folderPath);
        const foldernames = items.filter(item => {
            const itemPath = path.join(folderPath, item);
            
            // Check if the item is a directory and if it starts with the prefix
            return fs.statSync(itemPath).isDirectory() && item.startsWith(prefixname);
        });

        numIndexTotal = foldernames.length;
        numIndex = 1;
        let localIndexPaths = [];
        foldernames.forEach(foldername => {
                    if (!fs.existsSync(path.join(folderPath, `${foldername}`))) {
                        console.log(`${folderPath/foldername} doesn't exist, skip`)
                        return
                    }

                    let startTime = Date.now()
                    // Create batch index folder
                    const indexDirPath = path.join(folderPath, `index-${foldername}`)
                    if (!fs.existsSync(indexDirPath)) {
                        fs.mkdirSync(indexDirPath, { recursive: true });
                    }
            
                    // Create file to store index if it doesn't exist
                    const localIndexPath = path.join(folderPath, `index-${foldername}/index.json`)
                    localIndexPaths.push(localIndexPath);
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
                        let batchStartTime = Date.now();
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
                                updateIndex(localIndexPath, fileIndex);
            
                                if (counter == keys.length) {
                                    let batchEndTime = Date.now();
                                    let duration = batchEndTime - batchStartTime;
                                    // console.log(`Finished Indexing ${keys.length} websites in ${foldername}. Took ${duration}ms`)
                                    if (numIndex == numIndexTotal) {
                                        
                                        // Combining all the batched index into one index per node
                                        let combineCounter = 1;
                                        localIndexPaths.forEach((localIndexPath) => {
                                            // Create file to store index if it doesn't exist
                                            const globalIndexPath = path.join(folderPath, `index.json`)
                                            createFileIfNotExists(globalIndexPath, '{}');

                                            const data = fs.readFileSync(localIndexPath, 'utf8');
                                            const localIndex = JSON.parse(data);

                                            updateIndex(globalIndexPath, localIndex)

                                            if (combineCounter == localIndexPaths.length) {
                                                let endTime = Date.now();
                                                let duration = endTime - startTime;
                                                console.log(`Finished Indexing ${numIndexTotal} batches. Took ${duration}ms`)
                                            }
                                            combineCounter += 1;
                                        });

                                    }
                                    numIndex += 1;
                                }
                                counter += 1;
                            });
                        });
            
                        // console.log(`Starting Indexing ${keys.length} websites.`)
                    });
        })
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

