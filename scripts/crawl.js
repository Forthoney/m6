global.nodeConfig = { ip: "127.0.0.1", port: 7070 };

const assert = require("node:assert");
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

const n1 = { ip: "127.0.0.1", port: 7110 };
const n2 = { ip: "127.0.0.1", port: 7111 };
const n3 = { ip: "127.0.0.1", port: 7112 };

const crawlGroup = {
  [id.getSID(n1)]: n1,
  [id.getSID(n2)]: n2,
  [id.getSID(n3)]: n3,
};

function startNodes() {
  return Promise.all(
    Object.values(crawlGroup).map((n) =>
      distribution.local.status.spawnPromise(n),
    ),
  );
}

function doMapReduce() {
  distribution.crawl.store.getPromise(null).then((keys) => {
    distribution.crawl.mr.exec({ keys, map, reduce, id: "crawler" }, (e, v) => {
      console.error(e);
      assert(Object.values(e).length === 0);
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
distribution.node.start((server) => {
  localServer = server;
  const crawlConfig = { gid: "crawl" };
  startNodes().then(() => {
    groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
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
