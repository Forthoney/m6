const local = require("../local/local");
const { getSID } = require("../util/id");

const comm = (config) => {
  const context = {
    gid: config.gid || "all",
  };
  const mySid = getSID(global.nodeConfig);

  return {
    send: (message, remote, callback = () => {}) => {
      local.groups.get(context.gid, (e, group) => {
        if (e) {
          const err = {};
          err[mySid] = e;
          return callback(e, null);
        }

        const errors = {};
        const results = {};
        let count = 0;
        const entries = Object.entries(group);

        entries.forEach(([sid, node]) => {
          Object.assign(remote, { node: node });
          local.comm.send(message, remote, (e, v) => {
            if (e) {
              errors[sid] = e;
            } else {
              results[sid] = v;
            }

            if (++count === entries.length) {
              callback(errors, results);
            }
          });
        });
      });
    },
  };
};

module.exports = comm;
