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
 * @returns {object}
 */

/**
 * @typedef {Object} NodeInfo
 * @property {string} ip
 * @property {number} port
 */

/**
 * @typedef {Object.<string, NodeInfo>} Group
 */

module.exports = { Callback, Mapper, Reducer, NodeInfo };
