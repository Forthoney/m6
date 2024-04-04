const comm = require("./comm");

const routes = (config) => {
  return {
    put: (service, name, callback = () => {}) => {
      comm(config).send(
        [service, name],
        { service: "routes", method: "put" },
        callback,
      );
    },
  };
};

module.exports = routes;
