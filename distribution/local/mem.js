const id = require("../util/id");

const topLevelMem = new Map();
const groupMem = new Map();

function getFromGroupMem(currentGroupMem, key, callback) {
  if (key === null) return callback(null, Array.from(currentGroupMem.keys()));

  const val = currentGroupMem.get(key);
  if (val === undefined) {
    callback(Error(`Key ${key} not found`));
  } else {
    callback(null, val);
  }
}

function putToGroupMem(groupMem, key, val, callback) {
  if (key === null) {
    groupMem.set(id.getID(val), val);
  } else {
    groupMem.set(key, val);
  }
  callback(null, val);
}

function delFromGroupMem(groupMem, key, callback) {
  const val = groupMem.get(key);
  if (val === undefined) {
    callback(Error(`Key ${key} not found for del operation`));
  } else {
    groupMem.delete(key);
    callback(null, val);
  }
}

const mem = {
  get: (key, callback = () => {}) => {
    if (key === null || typeof key == "string") {
      getFromGroupMem(topLevelMem, key, callback);
    } else {
      if (!groupMem.has(key.gid)) {
        groupMem.set(key.gid, new Map());
      }
      getFromGroupMem(groupMem.get(key.gid), key.key, callback);
    }
  },

  put: (val, key, callback = () => {}) => {
    if (key === null || typeof key === "string") {
      putToGroupMem(topLevelMem, key, val, callback);
    } else {
      if (!groupMem.has(key.gid)) {
        groupMem.set(key.gid, new Map());
      }
      putToGroupMem(groupMem.get(key.gid), key.key, val, callback);
    }
  },

  del: (key, callback = () => {}) => {
    if (typeof key === "string") {
      delFromGroupMem(topLevelMem, key, callback);
    } else {
      const thisGroupMem = groupMem.get(key.gid);
      if (thisGroupMem === undefined) {
        return callback(Error(`GID ${key.gid} not found for del operation`));
      }

      delFromGroupMem(thisGroupMem, key.key, callback);
    }
  },
};

module.exports = mem;
