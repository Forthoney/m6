// @ts-check

const id = require("../util/id");
const local = require("../local/local");
const comm = require("./comm");
const types = require("../types");

/**
 * @param {object} config
 */
function store(config) {
  const context = {
    gid: config.gid || "all",
    hash: config.hash || id.naiveHash,
  };

  /**
   * @param {Object.<string, object>} group
   * @param {any} key
   */
  function groupToDestinationNode(group, key) {
    const nidToNodeMap = new Map(
      Object.values(group).map((node) => [id.getNID(node), node]),
    );
    const destinationNID = context.hash(id.getID(key), nidToNodeMap.keys());
    return nidToNodeMap[destinationNID];
  }

  /**
   * @param {?string} key
   * @param {types.Callback} callback
   */
  function get(key, callback = (_e, _) => {}) {
    const query = { key: key, gid: context.gid };
    if (key === null) {
      comm(config).send(
        [query],
        { service: "store", method: "get" },
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
          service: "store",
          method: "get",
          node: groupToDestinationNode(group, key),
        };
        local.comm.send([query], remote, callback);
      });
    }
  }

  /**
   * @param {any} val
   * @param {?string} key
   * @param {types.Callback} callback
   */
  function put(val, key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, null);

      const remote = {
        service: "store",
        method: "put",
        node: groupToDestinationNode(group, key || id.getID(val)),
      };
      local.comm.send([val, { key: key, gid: context.gid }], remote, callback);
    });
  }

  /**
   * @param {string} key
   * @param {types.Callback} callback
   */
  function del(key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, null);

      const remote = {
        service: "store",
        method: "del",
        node: groupToDestinationNode(group, key),
      };
      local.comm.send([{ key: key, gid: context.gid }], remote, callback);
    });
  }

  return { get, put, del };
}

module.exports = store;
