const id = require("../util/id");

const groupMap = new Map();

const groups = {
  put: (config, group, callback = () => {}) => {
    if (typeof group !== "object") {
      return callback(Error("Invalid group structure"), null);
    }

    config = typeof config === "string" ? { gid: config } : config;
    groupMap.set(config.gid, group);

    callback(null, group);
  },

  get: (name, callback = () => {}) => {
    if (groupMap.has(name)) {
      callback(null, groupMap.get(name));
    } else {
      callback(Error(`Could not find group with name ${name}`), null);
    }
  },

  del: (name, callback = () => {}) => {
    if (groupMap.has(name)) {
      const deleted = groupMap.get(name);
      groupMap.delete(name);
      callback(null, deleted);
    } else {
      callback(Error(`Could not find group with name ${name}`), null);
    }
  },

  add: (name, node, callback = () => {}) => {
    if (groupMap.has(name)) {
      const sid = id.getSID(node);
      groupMap.get(name)[sid] = node;
      callback(null, groupMap.get(name));
    } else {
      callback(Error(`Could not find group with name ${name}`), null);
    }
  },

  rem: function (name, sid, callback = () => {}) {
    if (groupMap.has(name) && sid in groupMap.get(name)) {
      delete groupMap.get(name)[sid];
      callback(null, groupMap.get(name));
    } else {
      callback(Error(`Could not find appropriate node to remove`), null);
    }
  },
};

module.exports = groups;
