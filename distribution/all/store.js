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

  function reconf(oldConfig, callback = () => {}) {
    console.log('oldConfig', oldConfig);

    // Step 1. Get current group config.
    distService.groups.get(context.gid, (e, v) => {
      let keys = Object.keys(v);
      console.log("Keys: ", keys);

      let firstKey = keys[0] || null;
      console.log("First Key?", firstKey);

      let currentConfig;

      if (firstKey != null) {
        currentConfig = v[firstKey];
      }

      console.log("Current  Config", currentConfig);

      // Step 2. Get all keys in current group.
      distService.store.get(null, (err, allKeys) => {
        allKeys = [...new Set(allKeys)];
        console.log("All Keys: ", allKeys)

        // Step 3: Identify which objects need to be relocated.
        let relocationTasks = [];
        allKeys.forEach((key) => {
          let kid = id.getID(key);

          // Use both old and new groups to hash the kid
          // and determine its target node.
          let oldNids = Object.values(oldConfig).map((node) =>
            id.getNID(node));

          console.log("oldNids", oldNids);

          let newNids = Object.values(currentConfig).map((node) =>
            id.getNID(node));

          console.log("newNids", newNids);

          let oldTargetNid = context.hash(kid, oldNids);
          let newTargetNid = context.hash(kid, newNids);

          console.log("old target nid", oldTargetNid);
          console.log("new target nid", newTargetNid);
          
          let targetConfig = oldConfig[oldTargetNid.substring(0, 5)];

          if (oldTargetNid !== newTargetNid) {
            relocationTasks.push({key, targetConfig});
          }
        });

        console.log('Different', relocationTasks);
      });

        callback();
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
    if (key === null) {
      return groupPromisify(get)(key);
    } else {
      return new Promise((resolve, reject) => {
        get(key, (e, v) => (e ? reject(e) : resolve(v)));
      });
    }
  };

  delGroup[promisify.custom] = groupPromisify(delGroup);

  return {
    get,
    put,
    del,
    delGroup,
    reconf,
    getPromise: promisify(get),
    delGroupPromise: promisify(delGroup),
  };
}

module.exports = store;
