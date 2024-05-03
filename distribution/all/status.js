const { promisify } = require("node:util");
const local = require("../local/local");

function status(config) {
  const context = {
    gid: config.gid || "all",
  };

  const distService = global.distribution[context.gid];

  function get(configuration, callback = () => {}) {
    distService.comm.send(
      [configuration],
      { service: "status", method: "get" },
      (e, v) => {
        if (Object.values(e).length !== 0) {
          return callback(e, {});
        }

        if (["counts", "heapTotal", "heapUsed"].includes(configuration)) {
          callback(
            e,
            Object.values(v).reduce((acc, val) => acc + val, 0),
          );
        } else {
          callback(e, v);
        }
      },
    );
  }

  function stop(callback = () => {}) {
    distService.comm.send([], { service: "status", method: "stop" }, (e, v) => {
      setTimeout(() => {
        callback(e, v);
      }, 1000);
    });
  }

  function spawn(configuration, callback = () => {}) {
    local.status.spawn(configuration, (e, node) => {
      if (e) {
        return callback(e, {});
      }

      distService.groups.add(config.gid, node, (e, _v) => {
        if (Object.keys(e).length === 0) {
          callback(null, node);
        } else {
          callback(e, null);
        }
      });
    });
  }

  return { get, stop, spawn, spawnPromise: promisify(spawn) };
}

module.exports = status;
