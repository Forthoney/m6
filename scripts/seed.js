const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");

const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

function map(_key, vUrl) {
  return new Promise((resolve, reject) => {
    let data = "";
    const https = require("node:https");
    https
      .get(vUrl, (res) => {
        const { statusCode } = res;
        if (statusCode !== 200) {
          res.resume();
          return reject(Error(`Request Failed with Error code ${statusCode}`));
        }
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve({ [vUrl]: data }));
      })
      .on("error", (e) => reject(e));
  });
}

function doMapReduce(callback) {
  distribution.crawl.store.getPromise(null).then((keys) => {
    distribution.crawl.mr.exec(
      { keys, map, id: "crawler", storeLocally: true },
      (e, v) => {
        console.log("COMPLETE=========================");
        callback();
      },
    );
  });
}

const urlsRaw = fs.readFileSync(
  path.join(__dirname, "..", "data", "urls.txt"),
  "utf8",
);
const urls = urlsRaw.split("\n").map((url, idx) => {
  return { [idx]: url };
});
console.log(urls[0]);

function seed(callback = () => {}) {
  let counter = 0;
  urls.forEach((url) => {
    const [key, val] = Object.entries(url)[0];
    distribution.crawl.store.put(val, key, (e, v) => {
      if (++counter == urls.length) {
        doMapReduce(callback);
      }
    });
  });
}

module.exports = { seed };
