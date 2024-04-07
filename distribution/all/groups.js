const comm = require("./comm");
const local = require("../local/local");
const { getSID } = require("../util/id");

const mySid = getSID(global.nodeConfig);

function groups(config) {
  function accumulateCommLocal(method, ...args) {
    const callback = args.pop();
    comm(config).send(args, { service: "groups", method: method }, (e, v) => {
      local.groups[method](...args, (e2, v2) => {
        if (e2) {
          e[mySid] = e2;
        } else {
          v[mySid] = v2;
        }
        callback(e, v);
      });
    });
  }

  return {
    get: (name, callback = () => {}) => {
      accumulateCommLocal("get", name, callback);
    },

    put: (newConfig, group, callback = () => {}) => {
      newConfig =
        typeof newConfig === "string" ? { gid: newConfig } : newConfig;

      global.distribution[newConfig.gid] = {
        status: require("./status")(newConfig),
        comm: require("./comm")(newConfig),
        groups: require("./groups")(newConfig),
        routes: require("./routes")(newConfig),
        gossip: require("./gossip")(newConfig),
        mem: require("./mem")(newConfig),
        store: require("./store")(newConfig),
        mr: require("./mr")(newConfig),
      };
      local.groups.put(newConfig, group, (e, newGroup) => {
        const err = {};
        if (e) {
          err[mySid] = e;
          return callback(err, {});
        }

        comm(config).send(
          [newConfig, newGroup],
          { service: "groups", method: "put" },
          (e, v) => {
            callback(e, v);
          },
        );
      });
    },

    del: (name, callback = () => {}) => {
      accumulateCommLocal("del", name, (e, v) => {
        delete global.distribution[name];
        callback(e, v);
      });
    },

    add: (name, node, callback = () => {}) => {
      accumulateCommLocal("add", name, node, callback);
    },

    rem: (name, sid, callback = () => {}) => {
      accumulateCommLocal("rem", name, sid, callback);
    },
  };
}

module.exports = groups;
