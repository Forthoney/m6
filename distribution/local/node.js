const http = require("http");
const url = require("url");

let local = require("../local/local");
const serialization = require("../util/serialization");

/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/

const nodeConfig = global.nodeConfig;

function isValidBody(body) {
  if (body.length === 0) {
    return new Error("No body");
  }

  try {
    JSON.parse(body);
    return false;
  } catch (error) {
    return error;
  }
}

const start = function (onStart) {
  const server = http.createServer((req, res) => {
    /* Your server will be listening for PUT requests. */

    // Write some code...

    if (req.method !== "PUT") {
      res.end(serialization.serialize(new Error("Method not allowed!")));
      return;
    }

    /*
      The path of the http request will determine the service to be used.
      The url will have the form: http://node_ip:node_port/service/method
    */

    // Write some code...

    const pathname = url.parse(req.url).pathname;
    const [, service, method] = pathname.split("/");

    console.log(`[SERVER] (${nodeConfig.ip}:${nodeConfig.port})
        Request: ${service}:${method}`);

    /*

      A common pattern in handling HTTP requests in Node.js is to have a
      subroutine that collects all the data chunks belonging to the same
      request. These chunks are aggregated into a body variable.

      When the req.on('end') event is emitted, it signifies that all data from
      the request has been received. Typically, this data is in the form of a
      string. To work with this data in a structured format, it is often parsed
      into a JSON object using JSON.parse(body), provided the data is in JSON
      format.

      Our nodes expect data in JSON format.
  */

    // Write some code...

    let body = [];

    req.on("data", (chunk) => {
      body.push(chunk);
    });

    req.on("end", () => {
      const stringBody = Buffer.concat(body).toString();

      const error = isValidBody(stringBody);
      if (error) {
        res.end(serialization.serialize([error, null]));
        return;
      }

      const jsBody = serialization.deserialize(stringBody);
      /* Here, you can handle the service requests. */

      const serviceCallback = (e, v) => {
        res.end(serialization.serialize([e, v]));
      };

      local.routes.get(service, (error, ser) => {
        if (error) {
          res.end(serialization.serialize([error, null]));
          console.error(error);
          return;
        }

        console.log(`[SERVER] ${service}:${method} Args: ${JSON.stringify(jsBody)} 
            ServiceCallback: ${serviceCallback}`);

        if (method in ser) {
          ser[method](...jsBody, serviceCallback);
        } else {
          return serviceCallback(
            Error(`invalid method ${method} for ${ser}`),
            null,
          );
        }
      });
    });
  });

  // Write some code...

  /*
    Your server will be listening on the port and ip specified in the config
    You'll be calling the onStart callback when your server has successfully
    started.

    In this milestone, we'll be adding the ability to stop a node
    remotely through the service interface.
  */

  server.listen(nodeConfig.port, nodeConfig.ip, () => {
    console.log(
      `Server running at http://${nodeConfig.ip}:${nodeConfig.port}/`,
    );
    if (process.send !== undefined) {
      process.send("spawned node running");
    }
    onStart(server);
  });
};

module.exports = {
  start: start,
};
