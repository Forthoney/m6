#!/usr/bin/env node

const util = require("./distribution/util/util.js");
const { getURLs } = require("./scripts/getURLs.js");
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
});

if (args.crawl) {
  global.nodeConfig.onStart = () => {
    const crawlGroup = { ip: "172.31.20.108", port: 7070 };
    const crawlConfig = { gid: "crawl" };
    const { seed } = require("./scripts/seed");
    const groupMaker = require("./distribution/all/groups")(crawlConfig);
    groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
      seed(() => getURLs());
    });
  };
}

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  distribution.node.start(global.nodeConfig.onStart);
}
