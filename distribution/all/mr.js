const local = require("../local/local");
const { toAsync, createRPC } = require("../util/wire");
const { getID, getNID } = require("../util/id");
const store = require("./store");
const comm = require("./comm");

function mapOnNodes(jobID, nodes, keys, mapper) {
  const neighborNIDs = nodes.map((node) => getNID(node));
  nodes.forEach((node, idx) => {
    const assignedKeys = keys.filter((key) => key % nodes.length == idx);
    local.comm.send(
      [jobID, mapper, assignedKeys, global.nodeConfig, neighborNIDs],
      {
        node: node,
        service: "mr",
        method: "map",
      },
      (e, _) => {
        if (e) throw e;
      },
    );
  });
}

function mr(config) {
  const context = {};
  context.gid = config.gid || "all";

  /*
   * Setup an notification endpoint. When workers are done mapping, they will
   * ping this endpoint. Once all workers are finished, the endpoint will trigger
   * the reduce phase
   */
  function setupNotifyEndpoint(jobID, numNotify, reducer, callback) {
    let completed = 0;
    const notify = () => {
      if (++completed == numNotify) {
        comm(config).send(
          [jobID, reducer],
          {
            service: "map",
            method: "reduce",
          },
          callback,
        );
      }
    };
    createRPC(toAsync(notify), (fnID = `mr-${jobID}`));
    return;
  }

  return {
    exec: (setting, callback = () => {}) => {
      if (setting.map == null || setting.reduce == null) {
        return callback(Error("Did not supply mapper or reducer"));
      }

      store(config).get(null, (e, keys) => {
        if (Object.values(e).length !== 0) return callback(e, {});

        local.groups.get(context.gid, (e, group) => {
          if (e) return callback(e, {});

          const jobID = setting.id || getID(setting);
          const nodes = Object.values(group);
          setupNotifyEndpoint(jobID, nodes.length, setting.reduce, callback);
          try {
            mapOnNodes(jobID, nodes, keys, setting.map);
          } catch (e) {
            callback(e);
          }
        });
      });
    },
  };
}

module.exports = mr;
