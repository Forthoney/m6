global.nodeConfig = { ip: "127.0.0.1", port: 7070 };

const fs = require("node:fs");
const path = require("node:path");
const distribution = require("../distribution");
const id = distribution.util.id;

const groupsTemplate = require("../distribution/all/groups");

const extractGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = { ip: "127.0.0.1", port: 7110 };
const n2 = { ip: "127.0.0.1", port: 7111 };
const n3 = { ip: "127.0.0.1", port: 7112 };

beforeAll((done) => {
  /* Stop the nodes if they are running */

  extractGroup[id.getSID(n1)] = n1;
  extractGroup[id.getSID(n2)] = n2;
  extractGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const extractConfig = { gid: "extract" };
    startNodes(() => {
      groupsTemplate(extractConfig).put(extractConfig, extractGroup, (e, v) => {
        done();
      });
    });
  });
});

afterAll((done) => {
  let remote = { service: "status", method: "stop" };
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) => {
    return mapper(Object.keys(o)[0], Object.values(o)[0]);
  });
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

test("URL extraction Test", (done) => {
  const map = (_key, value) => {
    const [url, text] = value;
    const { JSDOM } = require("jsdom");
    const { URL } = require("node:url");
    const dom = new JSDOM(text);
    result = [];
    dom.window.document.querySelectorAll("a").forEach((element) => {
      const relativeLink = element.href;
      let dirname = new URL(url).toString().replace("index.html", "");
      dirname += dirname.endsWith("/") ? "" : "/";
      result.push({ [dirname + relativeLink]: 0 });
    });
    return result;
  };
  const reduce = (key, value) => {
    return { [key]: 0 };
  };

  const sandbox1 = fs.readFileSync(
    path.join(__dirname, "testdata", "sandbox1.txt"),
    "utf8",
  );
  const urls = [
    {
      sandbox1: ["https://cs.brown.edu/courses/csci1380/sandbox/1/", sandbox1],
    },
  ];

  const expected = [
    {
      "https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/index.html": 0,
    },
    {
      "https://cs.brown.edu/courses/csci1380/sandbox/1/level_1b/index.html": 0,
    },
    {
      "https://cs.brown.edu/courses/csci1380/sandbox/1/level_1c/index.html": 0,
    },
  ];
  sanityCheck(map, reduce, urls, expected, done);

  const doMR = (cb) => {
    distribution.extract.store.get(null, (e, v) => {
      try {
        expect(e).toEqual({});
        expect(v.length).toBe(urls.length);
      } catch (e) {
        done(e);
      }

      distribution.extract.mr.exec({ keys: v, map, reduce }, (e, v) => {
        try {
          expect(e).toEqual({});
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let counter = 0;
  urls.forEach((o) => {
    const [filename, value] = Object.entries(o)[0];
    distribution.extract.store.put(value, filename, (e, v) => {
      expect(e).toEqual(null);
      if (++counter == urls.length) {
        doMR();
      }
    });
  });
});
test("(0 pts) sample test", () => {
  const t = true;
  expect(t).toBe(true);
});
test("(0 pts) sample test", () => {
  const t = true;
  expect(t).toBe(true);
});
test("(0 pts) sample test", () => {
  const t = true;
  expect(t).toBe(true);
});
test("(0 pts) sample test", () => {
  const t = true;
  expect(t).toBe(true);
});
