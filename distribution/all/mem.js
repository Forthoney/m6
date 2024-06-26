const id = require("../util/id");
const local = require("../local/local");

function mem(config) {
  const context = {
    gid: config.gid || "all",
    hash: config.hash || id.naiveHash,
  };

  const distService = global.distribution[context.gid];

  function groupToDestinationNode(group, key) {
    const nidToNodeMap = Object.fromEntries(
      Object.values(group).map((node) => [id.getNID(node), node]),
    );
    const destinationNID = context.hash(
      id.getID(key),
      Object.keys(nidToNodeMap),
    );
    return nidToNodeMap[destinationNID];
  }

  function get(key, callback = () => {}) {
    const query = { key: key, gid: context.gid };
    if (key === null) {
      distService.comm.send(
        [query],
        { service: "mem", method: "get" },
        (e, v) => {
          if (Object.values(e).length !== 0) return callback(e, {});

          const found = Object.values(v).reduce(
            (acc, val) => acc.concat(val),
            [],
          );
          callback(e, found);
        },
      );
    } else {
      local.groups.get(context.gid, (e, group) => {
        if (e) return callback(e, {});

        const remote = {
          service: "mem",
          method: "get",
          node: groupToDestinationNode(group, key),
        };
        local.comm.send([query], remote, callback);
      });
    }
  }
  function put(val, key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, null);

      const remote = {
        service: "mem",
        method: "put",
        node: groupToDestinationNode(group, key || id.getID(val)),
      };
      local.comm.send([val, { key: key, gid: context.gid }], remote, callback);
    });
  }

  function del(key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, null);

      const remote = {
        service: "mem",
        method: "del",
        node: groupToDestinationNode(group, key),
      };
      local.comm.send([{ key: key, gid: context.gid }], remote, callback);
    });
  }

  return { get, put, del };
}

module.exports = mem;
