/**
 * Callback function
 * @callback Callback
 * @param {(Error|Object.<any, Error>)} error
 * @param {any} [result=undefined]
 */

/**
 * Mapper function
 * @callback Mapper
 * @param {any} key
 * @param {any} value
 * @returns {object}
 */

/**
 * Mapper function
 * @callback Reducer
 * @param {any} key
 * @param {Array} value
 * @returns {object | object[]}
 */

/**
 * @typedef {Object} NodeInfo
 * @property {string} ip
 * @property {number} port
 */

/**
 * @typedef {Object.<string, NodeInfo>} Group
 */

/**
 * @typedef {object} MapReduceJobMetadata
 * @property {string} gid
 * @property {NodeInfo} supervisor
 * @property {string} jobID
 * @property {string} hash
 */

module.exports = {};
