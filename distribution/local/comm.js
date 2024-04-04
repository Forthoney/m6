const http = require("http");
const serialization = require("../util/serialization");

const comm = {
  send: (message, remote, continuation = () => {}) => {
    const msg = serialization.serialize(message);
    const options = {
      hostname: remote.node.ip,
      port: remote.node.port,
      path: `/${remote.service}/${remote.method}`,
      method: "PUT",
      headers: {
        "content-type": "application/json",
        "content-length": msg.length,
      },
    };
    const req = http.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => {
        body += chunk;
      });
      res.on("end", () => {
        continuation(...serialization.deserialize(body));
      });
    });

    req.on("error", (e) => {
      continuation(new Error("Error on Request"), null);
    });

    req.write(msg);
    req.end();
  },
};

module.exports = comm;
