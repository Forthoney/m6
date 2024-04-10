// @ts-check
/** @typedef {import("../types").Callback} Callback */

const comm = require('./comm');

/**
 * @param {object} config
 * @return {object}
 */
function routes(config) {
  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback = () => {}) {
    comm(config).send(
        [service, name],
        {service: 'routes', method: 'put'},
        callback,
    );
  }

  return {put};
}

module.exports = routes;
