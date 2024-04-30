const distribution = require("../distribution");

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
  return { [url]: null };
}

function getURLs(callback = () => {}) {
  distribution.crawl.store.getSubgroupPromise(null, "map-seed").then((keys) => {
    const subgroupKeys = keys.map((k) => `map-seed/${k}`);
    distribution.crawl.mr.exec(
      { keys: subgroupKeys, map, reduce, id: "getURLs" },
      (e, v) => {
        callback(v);
      },
    );
  });
}

module.exports = { getURLs };
