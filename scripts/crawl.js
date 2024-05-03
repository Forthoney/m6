const assert = require("node:assert");

const distribution = require("../distribution");

function map(_key, urls) {
  const https = require("node:https");
  const { URL } = require("node:url");

  const crawlPromises = Object.keys(urls).map((url) => {
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
            return reject(
              Error(`Request Failed with Error code ${statusCode}`),
            );
          }
          res.on("data", (chunk) => (data += chunk));
          res.on("end", () => {
            resolve({ [url]: data });
          });
        })
        .on("error", (e) => reject(e));
    });
  });
  return Promise.allSettled(crawlPromises).then((results) => {
    const success = {};
    for (const res of results) {
      if (res.status === "fulfilled") {
        Object.assign(success, res.value);
      }
    }
    return success;
  });
}

function crawl(callback = () => {}) {
  distribution.crawl.store
    .getSubgroupPromise(null, "reduce-getURLs")
    .then((keys) => {
      const subgroupKeys = keys.map((k) => `reduce-getURLs/${k}`);
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
          callback(v);
        },
      );
    });
}

module.exports = { crawl };
