const util = require("./distribution/util/util.js");

function initNodeConfig(args) {
  const nodeConfig = {
    ip: "127.0.0.1",
    port: 8080,
    onStart: () => console.log("Node started!"),
  };

  if (args.ip) {
    nodeConfig.ip = args.ip;
  }

  if (args.port) {
    nodeConfig.port = parseInt(args.port);
  }

  if (args.config) {
    const specificConfig = util.deserialize(args.config);
    Object.assign(nodeConfig, specificConfig);
  }

  if (args.aws) {
    const prevOnStart = nodeConfig.onStart;
    nodeConfig.onStart = () => {
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
  } else if (typeof args.local === "number") {
    const prevOnStart = nodeConfig.onStart;
    nodeConfig.onStart = () => {
      return new Promise((resolve, reject) => {
        prevOnStart();
        const nodes = [];
        const startingPortNum = nodeConfig.port;
        for (let i = 1; i <= args.local; i++) {
          nodes.push({ ip: nodeConfig.ip, port: startingPortNum + i * 10 });
        }
        Promise.all(
          nodes.map((n) => global.distribution.local.status.spawnPromise(n)),
        )
          .then((_) => {
            nodes.push(global.nodeConfig);
            resolve(nodes);
          })
          .catch((e) => reject(e));
      });
    };
  } else if (typeof args.spawner === "number") {
    nodeConfig.onStart = () => new Promise((res, rej) => res(args.spawner));
  }

  return nodeConfig;
}

function initDistribution() {
  global.distribution = {
    util: require("./distribution/util/util.js"),
    local: require("./distribution/local/local.js"),
    node: require("./distribution/local/node.js"),
    all: {},
  };
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

  return global.distribution;
}

module.exports = { initDistribution, initNodeConfig };
