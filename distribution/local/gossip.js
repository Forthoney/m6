const util = require('../util/util');
const groups = require('./groups');
const comm = require('./comm');

const seen = new Set();

const mySid = util.id.getSID(global.nodeConfig);

function getRandomElements(array, n) {
  const removedElements = [];

  for (let i = 0; i < n; i++) {
    const randomIndex = Math.floor(Math.random() * array.length);
    const removedElement = array.splice(randomIndex, 1)[0];
    removedElements.push(removedElement);
  }

  return removedElements;
}

const gossip = {
  at: (interval, func, callback = () => {}) => {
    callback(null, setInterval(func, interval));
  },
  del: (taskId, callback = () => {}) => {
    clearInterval(taskId);
    callback(null, true);
  },
  recv: (metadata, message, remote, callback = () => {}) => {
    if (seen.has(metadata.gossipId)) {
      return callback(null, true);
    }

    seen.add(metadata.gossipId);
    comm.send(message, {node: global.nodeConfig, ...remote}, (e, v) => {
      if (e) return callback(e, null);

      groups.get(metadata.gid, (e, group) => {
        if (e) return callback(e, null);

        const errors = {};
        const results = {};
        results[mySid] = v;
        let count = 0;

        const nodes = Object.entries(group).filter(([sid, _]) => sid != mySid);
        const selected = getRandomElements(nodes, Math.log(nodes.length));
        selected.forEach(([sid, node]) => {
          comm.send(
              [metadata, message, remote],
              {service: 'gossip', method: 'recv', node: node},
              (e, v) => {
                if (e != null) {
                  errors[sid] = e;
                } else {
                  results[sid] = v;
                }

                if (++count === selected.length) {
                  if (Object.values(errors).length === 0) {
                    callback(null, results);
                  } else {
                    callback(errors, results);
                  }
                }
              },
          );
        });
      });
    });
  },
};

module.exports = gossip;
