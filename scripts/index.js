const path = require("node:path");
const fs = require("node:fs");
const readline = require('node:readline');

// read file from folder
// change this to the path that contains mapReduce results
const folderPath = path.join(__dirname, "../test/parsed");
const indexPath = path.join(__dirname, "../test/index.json");

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

function updateGlobalIndex(newIndex) {
    try {
        const data = fs.readFileSync(indexPath, 'utf8');
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
        fs.writeFileSync(indexPath, JSON.stringify(globalIndex, null, 2));
    } catch (err) {
        console.error('Error reading JSON file:', err);
    }
}

function runIndexer() {
    // Create file to store index if it doesn't exist
    createFileIfNotExists(indexPath, '{}');

    try {
        const files = fs.readdirSync(folderPath);
        files.forEach(file => {
            const filePath = path.join(folderPath, file);
            
            var fileIndex = {};
            const lineReader = readline.createInterface({
                input: fs.createReadStream(filePath),
                crlfDelay: Infinity
            });

            let url;
            lineReader.on('line', (line) => {
                if (!url) {
                    url = line; // Save the first line as URL
                    console.log('URL:', url);
                } else {
                    const token = line.trim();
                    if (!(token in fileIndex)) {
                        fileIndex[token] = { count: 1, url: url };
                    } else {
                        fileIndex[token].count++;
                    }
                    // console.log('Line from file:', line);
                }
            });
        
            lineReader.on('close', () => {
                console.log('End of file reached.');
                console.log(fileIndex);
                // Store to the global index
                updateGlobalIndex(fileIndex);
                // TODO: delete original parsed file from folder to save memory

            })
        });
    } catch (err) {
        console.error('Error reading folder:', err);
    }
}

runIndexer()
