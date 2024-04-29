global.nodeConfig = { ip: "127.0.0.1", port: 7070 };

const { PerformanceObserver, performance } = require("node:perf_hooks");
const { argv } = require("node:process");
const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;

function startNodes(size) {
  const benchmarkGroup = {};
  for (let i = 0; i < size; i++) {
    const node = { ip: "127.0.0.1", port: 7110 + i };
    benchmarkGroup[id.getSID(node)] = node;
  }

  return Promise.all(
    Object.values(benchmarkGroup).map((n) =>
      distribution.local.status.spawnPromise(n),
    ),
  ).then((_) => benchmarkGroup);
}

const obs = new PerformanceObserver((items) => {
  console.log(items.getEntries());
});

const groupSize = parseInt(argv[2]);

obs.observe({ type: "measure" });

distribution.node.start(() => {
  const benchmarkConfig = { gid: "benchmark" };
  performance.measure("Start to pre-group creation");
  performance.mark("group-creation");
  startNodes(groupSize)
    .then((benchmarkGroup) => {
      groupMaker(benchmarkConfig).put(
        benchmarkConfig,
        benchmarkGroup,
        (e, v) => {
          performance.measure(
            `group creation for size ${groupSize}`,
            "group-creation",
          );
        },
      );
    })
    .finally((e) => {
      distribution.benchmark.status.stop((e, v) => {
        distribution.local.status.stop((e, v) => {});
      });
    });
});
