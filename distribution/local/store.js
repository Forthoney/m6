// @ts-check

const fs = require("node:fs");
const assert = require("node:assert");
const path = require("node:path");
const util = require("../util/util");
const { promisify } = require("node:util");

/** @typedef {import("../types").Callback} Callback */

// Top level store directory path
const storeDirpath = path.join(
  __dirname,
  "../../store",
  global.nodeConfig.port.toString(),
);

fs.mkdirSync(storeDirpath, { recursive: true });

/** @typedef {string} LocalKey */

/**
 * @typedef {Object} GroupKey
 * @property {string} gid
 * @property {?LocalKey} key
 */

/**
 * @typedef {Object} NestedGroupKey
 * @property {string} gid
 * @property {?string} folder
 * @property {?LocalKey} key
 */

/**
 * @callback FilePathCallback
 * @param {?Error} err
 * @param {string} [path=undefined]
 */

/**
 * @param {?LocalKey | GroupKey} key
 * @param {any} val
 * @param {FilePathCallback} callback
 * @return {void}
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
 * @callback DirCallback
 * @param {?Error} err
 * @param {string[]} [files=undefined]
 */

/**
 * @param {string} path
 * @param {DirCallback} callback
 * @return {void}
 */
function readDir(path, callback) {
  fs.readdir(path, { withFileTypes: true }, (err, files) => {
    if (err) {
      const wrappedErr = Error(err.message);
      wrappedErr["code"] = err.code;
      return callback(wrappedErr);
    } else {
      const filenames = files
        .filter((item) => item.isFile())
        .map((item) => item.name);
      callback(null, filenames);
    }
  });
}

/**
 * @param {string} path
 * @param {Callback} callback
 * @return {void}
 */
function readFile(path, callback) {
  fs.readFile(path, (err, file) => {
    err
      ? callback(Error(`Key ${path} not found in store`))
      : callback(null, util.deserialize(file));
  });
}

/**
 * @param {?LocalKey | ?NestedGroupKey | ?GroupKey} key
 * @param {Callback} callback
 * @return {void}
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
      const nestedKey = key.key;
      if (nestedKey["folder"] != null) {
        // Nested lookup
        if (nestedKey["key"] == null) {
          if (
            fs.existsSync(path.join(storeDirpath, key.gid, nestedKey["folder"]))
          ) {
            return readDir(
              path.join(storeDirpath, key.gid, nestedKey["folder"]),
              callback,
            );
          } else {
            return callback(null, []);
          }
        } else {
          return readFile(
            path.join(
              storeDirpath,
              key.gid,
              nestedKey["folder"],
              nestedKey["key"],
            ),
            callback,
          );
        }
      } else {
        return readFile(path.join(storeDirpath, key.gid, key.key), callback);
      }
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
 * @return {void}
 */
function getAll(gid, callback = () => {}) {
  readDir(path.join(storeDirpath, gid), (e, filenames) => {
    if (e) {
      return e["code"] === "ENOENT" ? callback(null, []) : callback(e);
    }

    const content = [];
    assert(filenames);
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
 * @param {Callback} callback
 * @return {void}
 */
function put(val, key, callback = () => {}) {
  resolveFilePath(key, val, (e, fullpath) => {
    if (e) {
      return callback(e);
    }

    const serialized = util.serialize(val);
    assert(fullpath);
    fs.writeFile(fullpath, serialized, (err) => {
      err ? callback(Error(err.message)) : callback(null, val);
    });
  });
}

/**
 * @callback DelGroupCallback
 * @param {?Error} err
 * @param {null} [val=undefined]
 */

/**
 * Deletes an entire group storage. Useful for deleting the intermediate
 * results of map reduce. Unlike store:del, this will not return what is
 * deleted.
 * @param {string} gid
 * @param {DelGroupCallback} callback
 * @return {void}
 */
function delGroup(gid, callback = () => {}) {
  fs.rm(path.join(storeDirpath, gid), { recursive: true, force: true }, (e) => {
    return e ? callback(Error(e.message)) : callback(null, null);
  });
}

/**
 * @param {LocalKey | GroupKey} key
 * @param {Callback} callback
 * @return {void}
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

module.exports = {
  get,
  getAll,
  put,
  del,
  delGroup,
  getPromise: promisify(get),
  getAllPromise: promisify(getAll),
  putPromise: promisify(put),
};
