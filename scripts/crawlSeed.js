global.nodeConfig = { ip: "127.0.0.1", port: 7070 };

const assert = require("node:assert");
const { PerformanceObserver, performance } = require("node:perf_hooks");
const fs = require("node:fs");
const path = require("node:path");

const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;

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

function reduce(kUrl, vData) {
  return { [kUrl]: vData };
}

crawlGroup = {};
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

const obs = new PerformanceObserver((items) => {
  console.log(items.getEntries());
  performance.clearMarks();
});

function doMapReduce() {
  performance.mark("mr-setup");
  distribution.crawl.store.getPromise(null).then((keys) => {
    distribution.crawl.mr.exec({ keys, map, reduce, id: "crawler" }, (e, v) => {
      performance.measure("MR end to end", "mr-setup");
      console.error(e);
      assert(Object.values(e).length === 0);
      console.log(v.length);
      console.log("COMPLETE=========================");
    });
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

let localServer = null;

obs.observe({ type: "measure" });

distribution.node.start((server) => {
  localServer = server;
  const crawlConfig = { gid: "crawl" };
  startNodes().then(() => {
    groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
      performance.measure("Start to group creation");
      assert(Object.values(e).length === 0);
      let counter = 0;
      urls.forEach((url) => {
        const [key, val] = Object.entries(url)[0];
        distribution.crawl.store.put(val, key, (e, v) => {
          if (++counter == urls.length) {
            doMapReduce();
          }
        });
      });
    });
  });
});
