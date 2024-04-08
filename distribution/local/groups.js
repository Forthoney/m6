// @ts-check
const id = require("../util/id");
const types = require("../types");

/** @type {Map.<string, types.Group>} */
const groupMap = new Map();

/**
 * @param {(object|string)} config
 * @param {types.Group} group
 * @param {types.Callback} callback
 */
function put(config, group, callback = (_e, _) => {}) {
  if (typeof group !== "object") {
    return callback(Error("Invalid group structure"));
  }

  config = typeof config === "string" ? { gid: config } : config;
  groupMap.set(config.gid, group);

  callback(null, group);
}

/**
 * @param {string} name
 * @param {types.Callback} callback
 */
function get(name, callback = () => {}) {
  if (groupMap.has(name)) {
    callback(null, groupMap.get(name));
  } else {
    callback(Error(`Could not find group with name ${name}`), null);
  }
}

/**
 * @param {string} name
 * @param {types.Callback} callback
 */
function del(name, callback = () => {}) {
  if (groupMap.has(name)) {
    const deleted = groupMap.get(name);
    groupMap.delete(name);
    callback(null, deleted);
  } else {
    callback(Error(`Could not find group with name ${name}`), null);
  }
}

/**
 * @param {string} name
 * @param {types.NodeInfo} node
 * @param {types.Callback} callback
 */
function add(name, node, callback = () => {}) {
  const group = groupMap.get(name);
  if (group) {
    const sid = id.getSID(node);
    group[sid] = node;
    callback(null, groupMap.get(name));
  } else {
    callback(Error(`Could not find group with name ${name}`), null);
  }
}

/**
 * @param {string} name
 * @param {id.ID} sid
 * @param {types.Callback} callback
 */
function rem(name, sid, callback = () => {}) {
  const group = groupMap.get(name);
  if (group && sid in group) {
    delete group[sid];
    callback(null, groupMap.get(name));
  } else {
    callback(Error(`Could not find appropriate node to remove`), null);
  }
}

module.exports = { put, get, add, del, rem };
