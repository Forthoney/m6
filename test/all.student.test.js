const https = require("node:https");
const { convert } = require("html-to-text");

global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const distribution = require("../distribution");
const id = distribution.util.id;

const groupsTemplate = require("../distribution/all/groups");

const crawlGroup = {};
const dlibGroup = {};

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

  crawlGroup[id.getSID(n1)] = n1;
  crawlGroup[id.getSID(n2)] = n2;
  crawlGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

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

    const crawlConfig = { gid: "crawl" };
    startNodes(() => {
      groupsTemplate(crawlConfig).put(crawlConfig, crawlGroup, (e, v) => {
        const dlibConfig = { gid: "dlib" };
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          done();
        });
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
    console.log(o);
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

test("Crawl Test", (done) => {
  const map = async (key, value) => {
    const promise = new Promise((resolve, reject) => {
      https.get(value, (res) => {
        const { statusCode } = res;
        const error = statusCode !== 200;
        if (error) {
          res.resume();
          reject(new Error(res.statusCode));
        }

        let body = "";
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          resolve(convert(body));
        });
        res.on("error", (err) => {
          reject(err);
        });
      });
    });
    return await promise;
  };
  const reduce = (key, value) => {
    distribution.crawl.store.get(value, (e, v) => {});
    return { [key]: value };
  };
  const urls = [{ 0: "https://cs.brown.edu/courses/csci1380/sandbox/1/" }];
  const expected = ["this"];
  sanityCheck(map, reduce, urls, expected, done);

  const doMR = (cb) => {
    distribution.crawl.store.get(null, (e, v) => {
      try {
        expect(e).toEqual({});
        console.log(v);
        expect(v.length).toBe(urls.length);
      } catch (e) {
        done(e);
      }

      distribution.crawl.mr.exec({ keys: v, map, reduce }, (e, v) => {
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
    const [key, value] = Object.entries(o)[0];
    distribution.crawl.store.put(value, key, (e, v) => {
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
