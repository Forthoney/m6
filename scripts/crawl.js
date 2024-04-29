global.nodeConfig = { ip: "127.0.0.1", port: 7070 };

const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");

const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;

function map(_key, vUrl) {
  const https = require("node:https");

  return new Promise((resolve, reject) => {
    let data = "";
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

function reduce(kUrl, vData) {
  return { [kUrl]: vData };
}

const crawlGroup = {};
for (let i = 0; i < 10; i++) {
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
        { keys: subgroupKeys, map, reduce, id: "crawl2" },
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
