const assert = require("node:assert");

const distribution = require("../distribution");

function map(_key, urls) {
  console.log(urls);
  const https = require("node:https");
  const { URL } = require("node:url");
  const assert = require("node:assert");

  const fetchPromises = urls.map((url) => {
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
  return Promise.allSettled(fetchPromises).then((results) => {
    const body = {};
    results.forEach((res) => {
      if (res.status === "fulfilled") {
        Object.assign(body, res.value);
      } else {
        console.log(urls);
        console.log(res.reason);
      }
    });
    return body;
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
          id: "crawl2",
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
