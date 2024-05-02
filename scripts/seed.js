const fs = require("node:fs");
const path = require("node:path");

const distribution = require("../distribution");

function map(_key, vUrl) {
  console.log(`REQUESTING ${vUrl}`);
  const https = require("node:https");

  function getCrawlDelay(baseUrl) {
    return new Promise((resolve, reject) => {
      const robotsUrl = new URL("/robots.txt", baseUrl);
  
      let robotsTxt = "";
      const req = https.get(robotsUrl, (res) => {
        if (res.statusCode === 200) {
          res.on("data", (chunk) => (robotsTxt += chunk));
          res.on("end", () => {
            // Default delay is 1 second
            let delay = 1;
            const lines = robotsTxt.split("\n");
            for (const line of lines) {
              const trimmedLine = line.trim();
              if (trimmedLine.toLowerCase().startsWith("crawl-delay") ||
                  trimmedLine.toLowerCase().startsWith("Crawl-delay")) {
                const parts = trimmedLine.split(":");
                if (parts.length === 2) {
                  delay = parseInt(parts[1].trim());
                  if (!isNaN(delay)) {
                    break;
                  }
                }
              }
            }
            console.log(`Crawl-delay: ${delay}`);
            resolve(delay);
          });
        } else {
          resolve(1);
        }
      });
  
      req.on("error", (e) => {
        console.error(`Error fetching robots.txt: ${e.message}`);
        resolve(5);
      });
      req.on("timeout", () => {
        req.destroy();
        resolve(1);
      });
      setTimeout(() => resolve(1));
    });
  }
  return getCrawlDelay(vUrl).then((crawlDelay) => {
    return new Promise((resolve, reject) => {
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
      // Wait for the crawl-delay suggested in robots.txt, default to 1s
      setTimeout(() => makeReq(vUrl), crawlDelay * 1000);
      // Wait for 10s to receive response
      setTimeout(() => reject(`SOCKET TIMOUT on ${vUrl}`), (crawlDelay + 10) * 1000);
    });
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

function seed(filename, callback = () => {}) {
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
            doMapReduce(filename, callback);
          }
        });
      });
    },
  );
}

module.exports = { seed };
