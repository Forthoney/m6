const id = require("../util/id");
const local = require("../local/local");

const mySid = id.getSID(global.nodeConfig);

function gossip(config) {
  const context = {
    gid: config.gid || "all",
  };

  const distService = global.distribution[context.gid];

  let gossipCounter = 0;

  return {
    at: (interval, func, callback = () => {}) => {
      distService.comm.send(
        [interval, func],
        { service: "gossip", method: "at" },
        (e, v) => {
          if (Object.values(e).length !== 0) {
            return callback(e, {});
          }
          callback(e, v);
        },
      );
    },
    del: (taskId, callback = () => {}) => {
      distService.comm.send(
        [taskId],
        { service: "gossip", method: "del" },
        (e, v) => {
          if (Object.values(e).length !== 0) {
            return callback(e, {});
          }
          callback(e, v);
        },
      );
    },
    send: (message, remote, callback = () => {}) => {
      local.groups.get(context.gid, (e, group) => {
        if (e) {
          const err = {};
          err[mySid] = e;
          return callback(e, null);
        }

        const [sid, node] = Object.entries(group)[0];
        local.comm.send(
          [{ gid: context.gid, gossipId: ++gossipCounter }, message, remote],
          { service: "gossip", method: "recv", node: node },
          (e, v) => {
            const errors = {};
            const results = {};
            if (e) {
              errors[sid] = e;
            } else {
              results[sid] = v;
            }
            callback(errors, results);
          },
        );
      });
    },
  };
}

module.exports = gossip;
