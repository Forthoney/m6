#!/usr/bin/env node

const args = require("yargs").argv;
const fs = require("node:fs");

// Default configuration
global.nodeConfig = global.nodeConfig || {
  ip: "127.0.0.1",
  port: 8080,
  onStart: () => console.log("Node started!"),
};

const id = require("./distribution/util/id");
const util = require("./distribution/util/util.js");

/*
As a debugging tool, you can pass ip and port arguments directly.
This is just to allow for you to easily startup nodes from the terminal.

Usage:
./distribution.js --ip '127.0.0.1' --port 1234
*/
if (args.ip) {
  global.nodeConfig.ip = args.ip;
}

if (args.port) {
  global.nodeConfig.port = parseInt(args.port);
}

if (args.config) {
  let nodeConfig = util.deserialize(args.config);
  global.nodeConfig.ip = nodeConfig.ip ? nodeConfig.ip : global.nodeConfig.ip;
  global.nodeConfig.port = nodeConfig.port
    ? nodeConfig.port
    : global.nodeConfig.port;
  global.nodeConfig.onStart = nodeConfig.onStart
    ? nodeConfig.onStart
    : global.nodeConfig.onStart;
}

const distribution = {
  util: require("./distribution/util/util.js"),
  local: require("./distribution/local/local.js"),
  node: require("./distribution/local/node.js"),
  all: {},
};

global.distribution = distribution;

Object.assign(global.distribution.all, {
  status: require("./distribution/all/status")({ gid: "all" }),
  comm: require("./distribution/all/comm")({ gid: "all" }),
  gossip: require("./distribution/all/gossip.js")({ gid: "all" }),
  groups: require("./distribution/all/groups.js")({ gid: "all" }),
  routes: require("./distribution/all/routes.js")({ gid: "all" }),
  mem: require("./distribution/all/mem.js")({ gid: "all" }),
  store: require("./distribution/all/store.js")({ gid: "all" }),
  mr: require("./distribution/all/mr.js")({ gid: "all" }),
});

if (args.aws) {
  const prevOnStart = global.nodeConfig.onStart;
  global.nodeConfig.onStart = () => {
    return new Promise((resolve, reject) => {
      prevOnStart();
      resolve([
        { ip: "172.31.27.16", port: 7090 },
        { ip: "172.31.24.189", port: 7090 },
        { ip: "172.31.31.146", port: 7090 },
        { ip: "172.31.27.16", port: 7080 },
        { ip: "172.31.24.189", port: 7080 },
        { ip: "172.31.31.146", port: 7080 },
        { ip: "172.31.27.16", port: 7070 },
        { ip: "172.31.24.189", port: 7070 },
        { ip: "172.31.31.146", port: 7070 },
      ]);
    });
  };
} else if (args.local) {
  const prevOnStart = global.nodeConfig.onStart;
  global.nodeConfig.onStart = () => {
    return new Promise((resolve, reject) => {
      prevOnStart();
      const nodes = [
        { ip: "127.0.0.1", port: 7070 },
        { ip: "127.0.0.1", port: 7071 },
        { ip: "127.0.0.1", port: 7072 },
        { ip: "127.0.0.1", port: 7073 },
        { ip: "127.0.0.1", port: 7074 },
        { ip: "127.0.0.1", port: 7075 },
        { ip: "127.0.0.1", port: 7076 },
        { ip: "127.0.0.1", port: 7077 },
        { ip: "127.0.0.1", port: 7078 },
        { ip: "127.0.0.1", port: 7079 },
        { ip: "127.0.0.1", port: 7080 },
      ];
      Promise.all(nodes.map((n) => distribution.local.status.spawnPromise(n)))
        .then((_) => {
          nodes.push(global.nodeConfig);
          resolve(nodes);
        })
        .catch((e) => reject(e));
    });
  };
}

if (args.aws || args.local) {
  const prevOnStart = global.nodeConfig.onStart;
  if (args.seed) {
    global.nodeConfig.onStart = () => {
      prevOnStart().then((nodes) => {
        const crawlGroup = {};
        for (const n of nodes) {
          crawlGroup[id.getSID(n)] = n;
        }
        const crawlConfig = { gid: "crawl" };
        const { seed } = require("./scripts/seed.js");
        const { getURLs } = require("./scripts/getURLs.js");
        const group = require("./distribution/all/groups.js")(crawlConfig);
        group.put(crawlConfig, crawlGroup, (e, v) => {
          console.log("FINISHPUT");
          seed(() => {
            console.log("Finished crawling seed");
            getURLs((v) => {
              const urls = Object.keys(v);
              console.log(`Found ${urls.length} outgoing links.`);
              fs.writeFile("outgoing-urls.txt", urls.join("\n"), () => {
                distribution.crawl.stop();
              });
            });
          });
        });
      });
    };
  } else if (args.crawl) {
    global.nodeConfig.onStart = () => {
      prevOnStart.then((nodes) => {
        const crawlGroup = {};
        for (const n of nodes) {
          crawlGroup[id.getSID(n)] = n;
        }
        const crawlConfig = { gid: "crawl" };
        const { crawl } = require("./scripts/crawl.js");
        const group = require("./distribution/all/groups.js")(crawlConfig);
        group.put(crawlConfig, crawlGroup, (e, v) => {
          crawl();
        });
      });
    };
  }
}

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  distribution.node.start(global.nodeConfig.onStart);
}
