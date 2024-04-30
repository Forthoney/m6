const assert = require("node:assert");

const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;

function map(_key, urls) {
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

const crawlGroup = {};
for (let i = 0; i < 500; i++) {
  const node = { ip: "127.0.0.1", port: 7110 + i };
  crawlGroup[id.getSID(node)] = node;
}

function startNodes() {
  return Promise.all(
    Object.values(crawlGroup).map((n) =>
      distribution.local.status.spawnPromise(n),
    ),
  );
}

function doMapReduce() {
  distribution.crawl.store
    .getSubgroupPromise(null, "reduce-getURLs")
    .then((keys) => {
      const subgroupKeys = keys.map((k) => `reduce-getURLs/${k}`);
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

let localServer = null;
distribution.node.start((server) => {
  localServer = server;
  const crawlConfig = { gid: "crawl" };
  startNodes().then(() => {
    groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
      doMapReduce();
    });
  });
});
