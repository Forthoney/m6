const assert = require("node:assert");
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
      .on("error", (e) => resolve(e));
  });
}

function reduce(kUrl, vData) {
  return { [kUrl]: vData };
}

global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
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
    Object.values(crawlGroup).map((node) =>
      distribution.local.status.spawnPromise(node),
    ),
  );
}

function doMapReduce() {
  distribution.crawl.store.getPromise(null).then((keys) => {
    distribution.crawl.mr.exec({ keys, map, reduce, async: true }, (e, v) => {
      assert(Object.values(e).length === 0);
      console.log("FINAL RESULT: ", v);
    });
  });
}

const urls = [{ 0: "https://example.com" }];

let localServer = null;
distribution.node.start((server) => {
  localServer = server;
  const crawlConfig = { gid: "crawl" };
  startNodes()
    .then((_) => {
      groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
        console.error("STARTNODE ERROR: ", e);
        assert(Object.values(e).length === 0);
        let counter = 0;
        urls.forEach((url) => {
          const [key, val] = Object.entries(url)[0];
          distribution.crawl.store.put(val, key, (e, v) => {
            console.error("PUT ERROR", e);
            if (++counter == urls.length) {
              doMapReduce();
            }
          });
        });
      });
    })
    .catch((e) => {
      console.error("FAILED TO SPAWN NODES", e);
    })
    .finally((_) => {
      Promise.allSettled(
        Object.values(crawlGroup).map((node) =>
          distribution.local.comm.sendPromise([], node),
        ),
      );
      localServer.close();
    });
});
