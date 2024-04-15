const https = require("node:https");

function map(_key, url) {
  let data = "";
  https.get(url, (res) => {
    res.on("data", (chunk) => (data += chunk));
    res.on("end");
  });
}
