const services = new Map();

const routes = {
  get: (service, callback = () => {}) => {
    if (services.has(service)) {
      return callback(null, services.get(service));
    } else if (global.rpcLocal.has(service)) {
      return callback(null, { call: global.rpcLocal.get(service) });
    } else {
      return callback(Error(`Route ${service} not found in services`), null);
    }
  },
  /*
   * Registers services under route
   * If route already exists, it will check to see if the route is extensible -
   * i.e. it is an object, not a singular value.
   * If it is an object (e.g. Object, Array), it will merge the existing
   * services under that route with the new services
   * Otherwise, it will return an error.
   */
  put: (service, route, callback = () => {}) => {
    services.set(route, service);
    return callback(null, services.get(route));
  },
};

services.set("status", require("./status"));
services.set("groups", require("./groups"));
services.set("comm", require("./comm"));
services.set("gossip", require("./gossip"));
services.set("mem", require("./mem"));
services.set("store", require("./store"));
services.set("mr", require("./mr"));
services.set("routes", routes);

module.exports = routes;
