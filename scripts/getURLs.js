global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const assert = require("node:assert");
const distribution = require("../distribution");
const groupMaker = require("../distribution/all/groups");

const id = distribution.util.id;

function map(filename, val) {
  const { JSDOM } = require("jsdom");
  const { URL } = require("node:url");
  const assert = require("node:assert");

  // This suppresses error messages thrown by JSDOM due to its broken
  // css parser. See https://github.com/jsdom/jsdom/issues/2177
  const originalConsoleError = console.error;
  const jsDomCssError = "Error: Could not parse CSS stylesheet";
  console.error = (...params) => {
    if (!params.find((p) => p.toString().includes(jsDomCssError))) {
      originalConsoleError(...params);
    }
  };

  const links = {};
  for (const [url, htmlWrapper] of Object.entries(val)) {
    assert(htmlWrapper.length === 1);
    const html = htmlWrapper[0];
    try {
      const dom = new JSDOM(html);
      for (const el of dom.window.document.querySelectorAll("a")) {
        let link = el.href;
        try {
          const concatLink = new URL(link, url);
          if (concatLink.origin === new URL(url).origin) {
            // relative URL
            link = concatLink.toString().replace("index.html", "");
          }

          link += link.endsWith("/") ? "" : "/";
          links[link] = null;
        } catch (e) {
          continue;
        }
      }
    } catch (e) {
      continue;
    }
  }

  return links;
}

function reduce(url, links) {
  return url;
}

const crawlGroup = {};
for (let i = 0; i < 500; i++) {
  const node = { ip: "127.0.0.1", port: 7110 + i };
  crawlGroup[id.getSID(node)] = node;
}

function startNodes() {
  return Promise.all(
    Object.values(crawlGroup).map((n) =>
      distribution.local.status.spawnPromise(n),
    ),
  );
}

function doMapReduce() {
  distribution.crawl.store
    .getSubgroupPromise(null, "map-crawler")
    .then((keys) => {
      const subgroupKeys = keys.map((k) => `map-crawler/${k}`);
      distribution.crawl.mr.exec(
        { keys: subgroupKeys, map, reduce, id: "getURLs" },
        (e, v) => {
          console.error(e);
          assert(Object.values(e).length === 0);
          console.log("FINAL RESULT: ", v);
        },
      );
    });
}

let localServer = null;
distribution.node.start((server) => {
  localServer = server;
  const crawlConfig = { gid: "crawl" };
  startNodes().then(() => {
    groupMaker(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
      doMapReduce();
    });
  });
});
