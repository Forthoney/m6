// @ts-check

const fs = require("node:fs");
const path = require("node:path");
const util = require("../util/util");
const types = require("../types");

// Top level store directory path
const storeDirpath = path.join(
  __dirname,
  "../../store",
  global.nodeConfig.port.toString(),
);

/**
 * @typedef {string} LocalKey
 */

/**
 * @typedef {Object} GroupKey
 * @property {string} gid
 * @property {?LocalKey} key
 */

/**
 * @param {?LocalKey | GroupKey} key
 * @param {any} val
 * @param {types.Callback} callback
 */
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

/**
 * @param {string} path
 * @param {types.Callback} callback
 */
function readDir(path, callback) {
  fs.readdir(path, (err, files) => {
    if (err) {
      const wrappedErr = Error(err.message);
      wrappedErr["code"] = err.code;
      return callback(wrappedErr);
    } else {
      callback(null, files);
    }
  });
}

/**
 * @param {string} path
 * @param {types.Callback} callback
 */
function readFile(path, callback) {
  fs.readFile(path, (err, file) => {
    err
      ? callback(Error(`Key ${path} not found in store`))
      : callback(null, util.deserialize(file));
  });
}

/**
 * @param {?LocalKey | GroupKey} key
 * @param {types.Callback} callback
 */
function get(key, callback = () => {}) {
  if (key === null) {
    return readDir(storeDirpath, callback);
  } else if (typeof key === "string") {
    return readFile(path.join(storeDirpath, key), callback);
  } else {
    if (key.key === null) {
      return readDir(path.join(storeDirpath, key.gid), (e, v) => {
        if (e) {
          return e["code"] === "ENOENT" ? callback(null, []) : callback(e);
        } else {
          return callback(null, v);
        }
      });
    } else {
      return readFile(path.join(storeDirpath, key.gid, key.key), callback);
    }
  }
}

/**
 * @callback GetAllCallback
 * @param {?Error} err
 * @param {Array | undefined} [values=undefined]
 */

/**
 * @param {string} gid
 * @param {GetAllCallback} callback
 */
function getAll(gid, callback = () => {}) {
  readDir(path.join(storeDirpath, gid), (e, filenames) => {
    if (e) {
      return e["code"] === "ENOENT" ? callback(null, []) : callback(e);
    }

    const content = [];
    const barrier = util.waitAll(filenames.length, (e) => {
      e ? callback(e) : callback(null, content);
    });

    filenames.forEach((file) => {
      readFile(path.join(storeDirpath, gid, file), (e, v) => {
        content.push(v);
        barrier(e);
      });
    });
  });
}

/**
 * @param {any} val
 * @param {?LocalKey | GroupKey} key
 * @param {types.Callback} callback
 */
function put(val, key, callback = () => {}) {
  resolveFilePath(key, val, (e, fullpath) => {
    if (e) return callback(e);

    const serialized = util.serialize(val);
    fs.writeFile(fullpath, serialized, { flush: true }, (err) => {
      err ? callback(Error(err.message)) : callback(null, val);
    });
  });
}

/**
 * @param {string} gid
 * @param {types.Callback} callback
 */
function delGroup(gid, callback = () => {}) {
  fs.rm(path.join(storeDirpath, gid), { recursive: true, force: true }, (e) => {
    return e ? callback(Error(e.message)) : callback(null, null);
  });
}

/**
 * @param {LocalKey | GroupKey} key
 * @param {types.Callback} callback
 */
function del(key, callback = () => {}) {
  let fullKey;
  if (typeof key === "string") {
    fullKey = key;
  } else {
    if (key["key"] == null) {
      return callback(Error("store:del called with null/invalid key"));
    } else {
      fullKey = path.join(key.gid, key.key);
    }
  }

  const filepath = path.join(storeDirpath, fullKey);
  fs.readFile(filepath, (readErr, file) => {
    if (readErr) return callback(Error(readErr.message));

    fs.unlink(filepath, (unlinkErr) => {
      unlinkErr
        ? callback(Error(unlinkErr.message))
        : callback(null, util.deserialize(file));
    });
  });
}

module.exports = { get, getAll, put, del, delGroup };
