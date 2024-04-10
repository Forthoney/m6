// @ts-check
/** @typedef {import("../types").Group} Group */
/** @typedef {import("../types").NodeInfo} NodeInfo */

const id = require('../util/id');

/** @type {Map.<string, Group>} */
const groupMap = new Map();

/**
 * @callback GroupCallback
 * @param {?Error} error
 * @param {Group} [group=undefined]
 */

/**
 * @param {(object|string)} config
 * @param {Group} group
 * @param {GroupCallback} callback
 * @return {void}
 */
function put(config, group, callback = (_e, _) => {}) {
  if (typeof group !== 'object') {
    return callback(Error('Invalid group structure'));
  }

  config = typeof config === 'string' ? {gid: config} : config;
  groupMap.set(config.gid, group);

  callback(null, group);
}

/**
 * @param {string} name
 * @param {GroupCallback} callback
 */
function get(name, callback = () => {}) {
  const group = groupMap.get(name);
  if (group) {
    callback(null, group);
  } else {
    callback(Error(`Could not find group with name ${name}`));
  }
}

/**
 * @param {string} name
 * @param {GroupCallback} callback
 */
function del(name, callback = () => {}) {
  const deleted = groupMap.get(name);
  if (deleted) {
    groupMap.delete(name);
    callback(null, deleted);
  } else {
    callback(Error(`Could not find group with name ${name}`));
  }
}

/**
 * @param {string} name
 * @param {NodeInfo} node
 * @param {GroupCallback} callback
 */
function add(name, node, callback = () => {}) {
  const group = groupMap.get(name);
  if (group) {
    const sid = id.getSID(node);
    group[sid] = node;
    callback(null, group);
  } else {
    callback(Error(`Could not find group with name ${name}`));
  }
}

/**
 * @param {string} name
 * @param {id.ID} sid
 * @param {GroupCallback} callback
 */
function rem(name, sid, callback = () => {}) {
  const group = groupMap.get(name);
  if (group && sid in group) {
    delete group[sid];
    callback(null, group);
  } else {
    callback(Error(`Could not find appropriate node to remove`));
  }
}

module.exports = {put, get, add, del, rem};
