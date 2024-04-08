// @ts-check
const comm = require("./comm");
const types = require("../types");

/**
 * @param {object} config
 */
function routes(config) {
  /**
   * @param {object} service
   * @param {string} name
   * @param {types.Callback} callback
   */
  function put(service, name, callback = () => {}) {
    comm(config).send(
      [service, name],
      { service: "routes", method: "put" },
      callback,
    );
  }

  return { put };
}

module.exports = routes;
