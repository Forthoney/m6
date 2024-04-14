// @ts-check
/** @typedef {import("../types").Callback} Callback */

/**
 * @param {object} config
 * @return {object}
 */
function routes(config) {
  const context = {
    gid: config.gid || "all",
  };

  const distService = global.distribution[context.gid];
  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback = () => {}) {
    distService.comm.send(
      [service, name],
      { service: "routes", method: "put" },
      callback,
    );
  }

  return { put };
}

module.exports = routes;
