const fs = require("node:fs");
const path = require("node:path");

const distribution = require("../distribution");

function map(_key, vUrl) {
  console.log(`REQUESTING ${vUrl}`);
  return new Promise((resolve, reject) => {
    const https = require("node:https");
    let data = "";
    const maxRedirects = 5;
    let redirectCount = 0;

    function makeReq(url) {
      const req = https
        .get(url, { timeout: 5000 }, (res) => {
          const { statusCode, headers } = res;
          if (statusCode >= 300 && statusCode < 400 && headers.location) {
            if (redirectCount++ < maxRedirects) {
              const redirect = new URL(headers.location, url);
              redirect.protocol = "https:";
              makeReq(redirect);
            } else {
              reject(Error("Max redirects exceeded"));
            }
          } else if (statusCode === 200) {
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
              console.log(`DONE WITH ${url}`);
              resolve({ [url]: data });
            });
          } else {
            res.resume();
            return reject(
              Error(`Request Failed with Error code ${statusCode}`),
            );
          }
        })
        .on("error", (e) => reject(e));
      req.on("timeout", () => {
        req.destroy();
        console.log(`TIMEOUT on ${vUrl}`);
        reject(Error("Timeout"));
      });
    }

    setTimeout(() => reject(`SOCKET TIMOUT on ${vUrl}`), 10000);
    makeReq(vUrl);
  });
}

function doMapReduce(filename, callback) {
  distribution.crawl.store.getPromise(null).then((keys) => {
    distribution.crawl.mr.exec(
      { keys, map, id: `seed-${filename}`, storeLocally: true },
      (e, v) => {
        callback();
      },
    );
  });
}

function seedRec(filenames, callback = () => {}) {
  if (filenames.length === 0) {
    return callback();
  }

  const [filename, ...tail] = filenames;
  fs.readFile(
    path.join(__dirname, "..", "data", filename),
    "utf8",
    (_e, urlsRaw) => {
      const urls = urlsRaw.split("\n").map((url, idx) => {
        return { [idx]: url };
      });

      let counter = 0;
      urls.forEach((url) => {
        const [key, val] = Object.entries(url)[0];
        distribution.crawl.store.put(val, key, (e, v) => {
          if (++counter == urls.length) {
            doMapReduce(filename, seedRec(tail, callback));
          }
        });
      });
    },
  );
}

function seed(namepattern, callback = () => {}) {
  fs.readdir(__dirname, "..", "data", (_e, filenames) => {
    const filtered = filenames.filter((n) => n.includes(namepattern));
    seed(filtered, callback);
  });
}

module.exports = { seed };
