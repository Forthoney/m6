// @ts-check
const id = require('../util/id');
const types = require('../types');

/** @type {Map.<string, types.Group>} */
const groupMap = new Map();

/**
 * @callback groupCallback
 * @param {?Error} error
 * @param {types.Group} [group=undefined]
 */

/**
 * @param {(object|string)} config
 * @param {types.Group} group
 * @param {groupCallback} callback
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
 * @param {groupCallback} callback
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
 * @param {groupCallback} callback
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
 * @param {types.NodeInfo} node
 * @param {groupCallback} callback
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
 * @param {groupCallback} callback
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
