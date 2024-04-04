const fs = require("node:fs");
const path = require("node:path");
const util = require("../util/util");

// Top level store directory path
const storeDirpath = path.join(__dirname, "../../store");

function resolveFilePath(key, val, callback) {
  if (key === null) {
    return callback(null, path.join(storeDirpath, util.id.getID(val)));
  } else if (typeof key === "string") {
    return callback(null, path.join(storeDirpath, key));
  } else {
    const groupPath = path.join(storeDirpath, key.gid);
    fs.mkdir(groupPath, { recursive: true }, (err) => {
      if (err) return callback(err);

      callback(null, path.join(groupPath, key.key || util.id.getID(val)));
    });
  }
}

function readDir(path, callback) {
  fs.readdir(path, (err, files) => {
    err ? callback(Error(err.message)) : callback(null, files);
  });
}

function readFile(path, callback) {
  fs.readFile(path, (err, file) => {
    err
      ? callback(Error(`Key not found`))
      : callback(null, util.deserialize(file));
  });
}

const store = {
  get: (key, callback = () => {}) => {
    if (key === null) {
      return readDir(storeDirpath, callback);
    } else if (typeof key === "string") {
      return readFile(path.join(storeDirpath, key), callback);
    } else {
      if (key.key === null) {
        return readDir(path.join(storeDirpath, key.gid), callback);
      } else {
        return readFile(path.join(storeDirpath, key.gid, key.key), callback);
      }
    }
  },

  put: (val, key, callback = () => {}) => {
    resolveFilePath(key, val, (e, fullpath) => {
      if (e) return callback(e);

      fs.writeFile(fullpath, util.serialize(val), (err) => {
        err ? callback(Error(err.message)) : callback(null, val);
      });
    });
  },

  del: (key, callback = () => {}) => {
    const fullKey = typeof key === "string" ? key : path.join(key.gid, key.key);
    const filepath = path.join(storeDirpath, fullKey);
    fs.readFile(filepath, (readErr, file) => {
      if (readErr) return callback(Error(readErr.message));

      fs.unlink(filepath, (unlinkErr) => {
        unlinkErr
          ? callback(Error(unlinkErr.message))
          : callback(null, util.deserialize(file));
      });
    });
  },
};

module.exports = store;
