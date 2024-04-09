// @ts-check

const assert = require("node:assert");
const id = require("../util/id");
const local = require("../local/local");
const comm = require("./comm");
const types = require("../types");

/**
 * @param {object} config
 */
function store(config) {
  /**
   * The setting for this store service
   * @type {object} context
   * @property {string} gid
   * @property {id.HashFunc} hash
   */
  const context = {
    gid: config.gid || "all",
    hash: config.hash || id.naiveHash,
  };

  /**
   * @param {types.Group} group
   * @param {string} key
   * @return {types.NodeInfo}
   */
  function groupToDestinationNode(group, key) {
    const nidToNodeMap = new Map(
      Object.values(group).map((node) => [id.getNID(node), node]),
    );
    const destinationNID = context.hash(
      id.getID(key),
      Array.from(nidToNodeMap.keys()),
    );

    const result = nidToNodeMap.get(destinationNID);
    assert(result);
    return result;
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

          const found = Object.values(v).flat();
          callback(e, found);
        },
      );
    } else {
      local.groups.get(context.gid, (e, group) => {
        if (e) return callback(e, {});

        assert(group);
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
      if (e) return callback(e, {});

      assert(group);
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
      if (e) return callback(e, {});

      assert(group);
      const remote = {
        service: "store",
        method: "del",
        node: groupToDestinationNode(group, key),
      };
      local.comm.send([{ key: key, gid: context.gid }], remote, callback);
    });
  }

  function delGroup(gid, callback = () => {}) {
    comm(config).send(
      [gid],
      { service: "store", method: "delGroup" },
      callback,
    );
  }

  return { get, put, del, delGroup };
}

module.exports = store;
