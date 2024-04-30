#!/usr/bin/env node

const id = require("./distribution/util/id");
const args = require("yargs").argv;

// Default configuration
global.nodeConfig = global.nodeConfig || {
  ip: "127.0.0.1",
  port: 8080,
  onStart: () => console.log("Node started!"),
};

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

if (args.crawl) {
  global.nodeConfig.onStart = () => {
    const nodes = [
      { ip: "172.31.27.16", port: 7090 },
      { ip: "172.31.24.189", port: 7090 },
      { ip: "172.31.31.146", port: 7090 },
      { ip: "172.31.27.16", port: 7080 },
      { ip: "172.31.24.189", port: 7080 },
      { ip: "172.31.31.146", port: 7080 },
      { ip: "172.31.27.16", port: 7070 },
      { ip: "172.31.24.189", port: 7070 },
      { ip: "172.31.31.146", port: 7070 },
    ];
    const crawlGroup = {};
    for (const n of nodes) {
      crawlGroup[id.getSID(n)] = n;
    }
    const crawlConfig = { gid: "crawl" };
    const { seed } = require("./scripts/seed");
    const { getURLs } = require("./scripts/getURLs.js");
    const group = require("./distribution/all/groups")(crawlConfig);
    group.put(crawlConfig, crawlGroup, (e, v) => {
      seed(() => {
        console.log("Finished crawling seed");
        getURLs();
      });
    });
  };
}

if (args.crawl2) {
  global.nodeConfig.onStart = () => {
    const nodes = [
      { ip: "172.31.27.16", port: 7090 },
      { ip: "172.31.24.189", port: 7090 },
      { ip: "172.31.31.146", port: 7090 },
      { ip: "172.31.27.16", port: 7080 },
      { ip: "172.31.24.189", port: 7080 },
      { ip: "172.31.31.146", port: 7080 },
      { ip: "172.31.27.16", port: 7070 },
      { ip: "172.31.24.189", port: 7070 },
      { ip: "172.31.31.146", port: 7070 },
    ];
    const crawlGroup = {};
    for (const n of nodes) {
      crawlGroup[id.getSID(n)] = n;
    }
    const crawlConfig = { gid: "crawl" };
    const { crawl } = require("./scripts/crawl.js");
    const group = require("./distribution/all/groups")(crawlConfig);
    group.put(crawlConfig, crawlGroup, (e, v) => {
      crawl();
    });
  };
}

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  distribution.node.start(global.nodeConfig.onStart);
}
