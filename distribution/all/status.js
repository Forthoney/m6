const comm = require("./comm");
const groups = require("./groups");
const local = require("../local/local");

const status = (config) => {
  return {
    get: (configuration, callback = () => {}) => {
      comm(config).send(
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
    },
    stop: (callback = () => {}) => {
      comm(config).send([], { service: "status", method: "stop" }, (e, v) => {
        setTimeout(() => {
          callback(e, v);
        }, 1000);
      });
    },
    spawn: (configuration, callback = () => {}) => {
      local.status.spawn(configuration, (e, node) => {
        if (e) {
          return callback(e, {});
        }

        groups(config).add(config.gid, node, (e, _v) => {
          if (Object.keys(e).length === 0) {
            callback(null, node);
          } else {
            callback(e, null);
          }
        });
      });
    },
  };
};

module.exports = status;
