// @ts-check

const fs = require("node:fs");
const assert = require("node:assert");
const path = require("node:path");
const util = require("../util/util");

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
 * @param {?LocalKey | GroupKey} key
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
      return readFile(path.join(storeDirpath, key.gid, key.key), callback);
    }
  }
}

/**
 * Searches for URLs associated with the provided query term from the index.json file, 
 * applying filters based on include and exclude URL lists.
 * @param {string} searchTerm - The query term to search for in the index.
 * @param {string[]} includeURLs - Array of URLs to include in the results.
 * @param {string[]} excludeURLs - Array of URLs to exclude from the results.
 * @param {Callback} callback - The callback function to return the results or errors.
 * @return {void}
 */
function query(searchTerm, includeURLs, excludeURLs, callback = () => {}) {
  const indexPath = path.join(__dirname, "../..", 'test', 'index.json');

  fs.readFile(indexPath, 'utf8', (err, data) => {
    if (err) {
      return callback(Error("Failed to read index file: " + err.message));
    }

    let index;
    try {
      index = JSON.parse(data);
    } catch (parseError) {
      return callback(Error("Failed to parse index file: " + parseError.message));
    }

    const results = index[searchTerm];
    if (!results) {
      return callback(null, []);  // Return null if no term matches.
    }

    // First apply the include filter
    let filteredResults = results;
    if (includeURLs.length > 0) {
      filteredResults = filteredResults.filter(item => includeURLs.includes(item.url));
    }

    // Then apply the exclude filter
    if (excludeURLs.length > 0) {
      filteredResults = filteredResults.filter(item => !excludeURLs.includes(item.url));
    }

    callback(null, filteredResults);
  });
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

module.exports = { get, getAll, put, del, query, delGroup };
