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
      const group = require("./distribution/all/groups.js")(crawlConfig);
      group.put(crawlConfig, crawlGroup, (e, v) => {
        seed(args.filename, () => {
          global.distribution.crawl.status.stop();
        });
      });
    });
  };
} else if (args.index) {
  if (!args.aws && !args.local) {
    throw Error("--aws or --local must be set to run crawl workflow");
  }

  if (typeof args.prefixname !== "string") {
    throw Error("--prefixname must be specified");
  }

  const prevOnStart = global.nodeConfig.onStart;
  global.nodeConfig.onStart = () => {
    prevOnStart().then((nodes) => {
      const crawlGroup = {};
      for (const n of nodes) {
        crawlGroup[id.getSID(n)] = n;
      }

      const crawlConfig = { gid: "crawl" };
      const { index } = require("./scripts/index.js");
      const group = require("./distribution/all/groups.js")(crawlConfig);
      group.put(crawlConfig, crawlGroup, (e, v) => {
        // Define service
        const boogleService = {};
        boogleService.index = index;

        distribution.crawl.routes.put(
          boogleService,
          "boogleService",
          (e, v) => {
            const path = require("node:path");
            const fs = require("node:fs");
            // Pre-define paths for internal storage
            const stopWordsPath = path.join(__dirname, "/data/stopwords.txt");
            const remote = { service: "boogleService", method: "index" };
            distribution.crawl.comm.send(
              [args.prefixname, stopWordsPath],
              remote,
              (e, v) => {
                console.log(e);
                console.log("COMPLETE INDEXING=========================");
                distribution.crawl.status.stop();
              },
            );
          },
        );
      });
    });
  };
} else if (args.spawner) {
  const prevOnStart = global.nodeConfig.onStart;
  global.nodeConfig.onStart = () => {
    prevOnStart().then((nWorker) => {
      const obs = new PerformanceObserver((items) => {
        console.log(items);
        performance.clearMarks();
      });

      const workerNodes = Array.from({ length: nWorker }, (_, i) => {
        return {
          ip: global.nodeConfig.ip,
          port: global.nodeConfig.port + 1 + i,
        };
      });

      obs.observe({ type: "measure" });
      performance.mark("init");
      Promise.all(
        workerNodes.map((config) =>
          global.distribution.local.status.spawnPromise(config),
        ),
      )
        .then((_) => {
          performance.measure("All children spawned", "init");
        })
        .catch((_) => console.log("UNEXPECTED ERR"))
        .finally((_) =>
          Promise.all(
            workerNodes.map((config) =>
              global.distribution.local.comm.sendPromise([], {
                node: config,
                service: "status",
                method: "stop",
              }),
            ),
          ).then(() => global.distribution.local.status.stop()),
        );
    });
  };
}

module.exports = global.distribution;

/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  global.distribution.node.start(global.nodeConfig.onStart);
}
