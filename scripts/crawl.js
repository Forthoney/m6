const assert = require("node:assert");

const distribution = require("../distribution");

function map(_key, urls) {
  const https = require("node:https");
  const { URL } = require("node:url");
  const assert = require("node:assert");

  assert(Object.keys(urls).length === 1);
  const url = Object.keys(urls)[0];
  return new Promise((resolve, reject) => {
    try {
      new URL(url);
    } catch (e) {
      return reject(e);
    }

    let data = "";
    https
      .get(url, (res) => {
        const { statusCode } = res;
        if (statusCode !== 200) {
          res.resume();
          return reject(Error(`Request Failed with Error code ${statusCode}`));
        }
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          resolve({ [url]: data });
        });
      })
      .on("error", (e) => reject(e));
  });
}

function crawl() {
  distribution.crawl.store
    .getSubgroupPromise(null, "map-getURLs")
    .then((keys) => {
      const subgroupKeys = keys.map((k) => `map-getURLs/${k}`);
      distribution.crawl.mr.exec(
        {
          keys: subgroupKeys,
          map,
          id: "crawl",
          storeLocally: true,
        },
        (e, v) => {
          console.error(e);
          assert(Object.values(e).length === 0);
          console.log("COMPLETE=========================");
        },
      );
    });
}

module.exports = { crawl };
