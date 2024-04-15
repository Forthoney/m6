// @ts-check
/** @typedef {import("../types").NodeInfo} NodeInfo */
/** @typedef {import("../types").Group} Group */
/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../local/store").LocalKey} LocalKey} */
/** @typedef {import("../local/store").GroupKey} GroupKey} */

const assert = require("node:assert");
const { id, groupPromisify } = require("../util/util");
const local = require("../local/local");
const { promisify } = require("node:util");

/**
 * @param {object} config
 * @return {object}
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
    hash: config.hash || id.consistentHash,
  };

  const distService = global.distribution[context.gid];

  /**
   * @param {Group} group
   * @param {string} key
   * @return {NodeInfo}
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
   * @param {?LocalKey} key
   * @param {Callback} callback
   * @return {void}
   */
  function get(key, callback = () => {}) {
    const query = { key: key, gid: context.gid };
    if (key === null) {
      distService.comm.send(
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
        if (e) {
          return callback(e, {});
        }

        assert(group);
        const destination = groupToDestinationNode(group, key);
        const remote = {
          service: "store",
          method: "get",
          node: destination,
        };
        local.comm.send([query], remote, callback);
      });
    }
  }

  /**
   * @param {any} val
   * @param {?LocalKey} key
   * @param {Callback} callback
   * @return {void}
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
   * @param {Callback} callback
   * @return {void}
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

  /**
   * @param {string} gid
   * @param {Callback} callback
   * returns {void}
   */
  function delGroup(gid, callback = () => {}) {
    distService.comm.send(
      [gid],
      { service: "store", method: "delGroup" },
      callback,
    );
  }

  get[promisify.custom] = (key) => {
    key === null ? groupPromisify(get)(key) : promisify(get)(key);
  };

  delGroup[promisify.custom] = groupPromisify(delGroup);

  return {
    get,
    put,
    del,
    delGroup,
    getPromise: promisify(get),
    delGroupPromise: promisify(delGroup),
  };
}

module.exports = store;
