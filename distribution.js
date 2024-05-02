#!/usr/bin/env node

const args = require("yargs").argv;
const fs = require("node:fs");
const setup = require("./setup.js");

global.nodeConfig = setup.initNodeConfig(args);
global.distribution = setup.initDistribution();

const id = require("./distribution/util/id");

/*
As a debugging tool, you can pass ip and port arguments directly.
This is just to allow for you to easily startup nodes from the terminal.

Usage:
./distribution.js --ip '127.0.0.1' --port 1234
*/

if (args.crawl) {
  if (!args.aws && !args.local) {
    throw Error("--aws or --local must be set to run crawl workflow");
  }

  if (typeof args.filename !== "string") {
    throw Error("--filename must be specified");
  }

  const prevOnStart = global.nodeConfig.onStart;
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
        seed(args.filename, () => {
          global.distribution.crawl.status.stop();
        });
      });
    });
  };
}

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  global.distribution.node.start(global.nodeConfig.onStart);
}
