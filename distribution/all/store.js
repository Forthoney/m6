// @ts-check
/** @typedef {import("../types").NodeInfo} NodeInfo */
/** @typedef {import("../types").Group} Group */
/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../local/store").LocalKey} LocalKey} */
/** @typedef {import("../local/store").GroupKey} GroupKey} */

const assert = require("node:assert");
const { id, groupPromisify } = require("../util/util");
const local = require("../local/local");
const { promisify } = require("node:util");

/**
 * @param {object} config
 * @return {object}
 */
function store(config) {
  /**
   * The setting for this store service
   * @type {object} context
   * @property {string} gid
   * @property {id.HashFunc} hash
   */
  const context = {
    gid: config.gid || "all",
    hash: config.hash || id.consistentHash,
  };

  const distService = global.distribution[context.gid];

  /**
   * @param {Group} group
   * @param {string} key
   * @return {NodeInfo}
   */
  function groupToDestinationNode(group, key) {
    const nidToNodeMap = new Map(
      Object.values(group).map((node) => [id.getNID(node), node]),
    );
    const destinationNID = context.hash(
      id.getID(key),
      Array.from(nidToNodeMap.keys()),
    );

    const result = nidToNodeMap.get(destinationNID);
    assert(result);
    return result;
  }

  /**
   * @param {?LocalKey} key
   * @param {Callback} callback
   * @return {void}
   */
  function get(key, callback = () => {}) {
    const query = { key: key, gid: context.gid };
    if (key === null) {
      distService.comm.send(
        [query],
        { service: "store", method: "get" },
        (e, v) => {
          if (Object.values(e).length !== 0) return callback(e, {});

          const found = Object.values(v).flat();
          callback(e, found);
        },
      );
    } else {
      local.groups.get(context.gid, (e, group) => {
        if (e) {
          return callback(e, {});
        }

        assert(group);
        const destination = groupToDestinationNode(group, key);
        const remote = {
          service: "store",
          method: "get",
          node: destination,
        };
        local.comm.send([query], remote, callback);
      });
    }
  }

  /**
   * @param {any} val
   * @param {?LocalKey} key
   * @param {Callback} callback
   * @return {void}
   */
  function put(val, key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, {});

      assert(group);
      const remote = {
        service: "store",
        method: "put",
        node: groupToDestinationNode(group, key || id.getID(val)),
      };
      local.comm.send([val, { key: key, gid: context.gid }], remote, callback);
    });
  }

  /**
   * @param {string} key
   * @param {Callback} callback
   * @return {void}
   */
  function del(key, callback = () => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, {});

      assert(group);
      const remote = {
        service: "store",
        method: "del",
        node: groupToDestinationNode(group, key),
      };
      local.comm.send([{ key: key, gid: context.gid }], remote, callback);
    });
  }

/**
 * Performs a distributed query across all nodes in the group for multiple search terms, aggregates results,
 * combines counts for identical URLs, and sorts them by count.
 * @param {string[]} searchTerms - The terms to query.
 * @param {string[]} includeURLs - URLs to include in the results.
 * @param {string[]} excludeURLs - URLs to exclude from the results.
 * @param {number} maxResults - Maximum number of result URLs to return.
 * @param {Callback} callback - Callback to handle the response or error.
 * @return {void}
 */
  function query(searchTerms, includeURLs, excludeURLs, maxResults, callback) {
    local.groups.get(context.gid, async (err, group) => {
      if (err) return callback(err);

      // Collect promises for each search term across all nodes
      const queryPromises = searchTerms.flatMap(term => {
        return Object.values(group).map(node => {
          return new Promise((resolve, reject) => {
            const remote = {
              service: "store",
              method: "query",
              node: node
            };
            
            local.comm.send([natural.PorterStemmer.stem(term), includeURLs, excludeURLs], remote, (e, result) => {
              if (e) {
                reject(e);
              } else {
                resolve(result);
              }
            });
          });
        });
      });

      // Wait for all queries to complete
      try {
        const results = await Promise.all(queryPromises);
        const urlCounts = {};

        // Aggregate counts from all search terms
        results.forEach(nodeResults => {
          nodeResults.forEach(item => {
            if (item && item.url) {
              urlCounts[item.url] = (urlCounts[item.url] || 0) + item.count;
            }
          });
        });

        // Convert the aggregated results into an array, sort by count
        const sortedUrls = Object.keys(urlCounts)
          .map(url => ({ url, count: urlCounts[url] }))
          .sort((a, b) => b.count - a.count)
          .map(item => item.url); // Extract only the URL, discard the count

        if (sortedUrls.length === 0) {
          callback(null, []); // No results found
        } else {
          callback(null, sortedUrls.slice(0, maxResults)); // Return top results based on maxResults
        }
      } catch (error) {
        callback(error);
      }
    });
  }

  // TODO: Delete moved key-value pairs.
  // TODO: Reconf any values that need to be moved between nodes that WEREN'T REMOVED.
  // TODO: Why the difference in expected vs real node assignments?
  function reconf(oldConfig, callback = () => {}) {
    // Step 1. Get current group config.
    distService.groups.get(context.gid, (e, v) => {
      let keys = Object.keys(v);
      let firstKey = keys[0] || null;
      let currentConfig;
  
      // Check for a valid configuration.
      if (firstKey != null) {
        currentConfig = v[firstKey];
      }
  
      // Step 2. Get all keys in current group.
      distService.store.get(null, (err, allKeys) => {
        allKeys = [...new Set(allKeys)];
  
        // Step 3. Identify removed node(s) & add their keys to the list.
        let missingInNewConfig = {};
  
        // Iterate over each node in oldConfig.
        Object.keys(oldConfig).forEach(key => {
          if (!currentConfig[key]) {
            // If a node in oldConfig is not present in currentConfig, add it to the result
            missingInNewConfig[key] = oldConfig[key];
          }
        });
  
        console.log("missingInNewConfig", missingInNewConfig);
  
        // Create an array of promises for handling missing nodes
        let handleMissingNodes = Object.values(missingInNewConfig).map(node => {
          return new Promise((resolve, reject) => {
            let remote = { node: node, service: 'store', method: 'get' };
            let message = [{ 'key': null, 'gid': context.gid }];
            local.comm.send(message, remote, (getErr, removedKeys) => {
              if (getErr) {
                reject(getErr);
              } else {
                console.log('VALUES FROM MISSING GET', removedKeys);
  
                // Process each key retrieved and update it in the store.
                let updateTasks = removedKeys.map(key => {
                  return new Promise((resolveUpdate, rejectUpdate) => {
                    remote = { node: node, service: 'store', method: 'get' };
                    message = [{ 'key': key, 'gid': context.gid }];
                    local.comm.send(message, remote, (getErr, valueToPut) => {
                      console.log(key, "Value to put!", valueToPut);
                      if (getErr) {
                        rejectUpdate(getErr);
                      } else {
                        distService.store.put(valueToPut, key, (putErr, putResult) => {
                          if (putErr) {
                            rejectUpdate(putErr);
                          } else {
                            console.log("Updated key", key, "with result", putResult);
                            resolveUpdate(putResult);
                          }
                        });
                      }
                    });
                  });
                });
  
                // Wait for all update tasks to complete
                Promise.all(updateTasks)
                  .then(updateResults => {
                    console.log("All updates completed for node", node);
                    resolve(updateResults);
                  })
                  .catch(updateError => {
                    console.log("Error updating data for node", node, updateError);
                    reject(updateError);
                  });
              }
            });
          });
        });
  
        // Wait for all missing node processes to complete
        Promise.all(handleMissingNodes).then(() => {
          distService.store.get(null, (err, allKeys2) => {
            console.log("ALLLLLLLLLLL GROUP KEYS POST RECONF", allKeys2);
            callback(); // Ensure callback is called here
          });
        }).catch(error => {
          console.error("Error handling missing nodes:", error);
          callback();
        });
      });
    });
  }
  

  /**
   * @param {string} gid
   * @param {Callback} callback
   * returns {void}
   */
  function delGroup(gid, callback = () => {}) {
    distService.comm.send(
      [gid],
      { service: "store", method: "delGroup" },
      callback,
    );
  }

  get[promisify.custom] = (key) => {
    if (key === null) {
      return groupPromisify(get)(key);
    } else {
      return new Promise((resolve, reject) => {
        get(key, (e, v) => (e ? reject(e) : resolve(v)));
      });
    }
  };

  delGroup[promisify.custom] = groupPromisify(delGroup);

  return {
    get,
    put,
    del,
    query,
    delGroup,
    reconf,
    getPromise: promisify(get),
    delGroupPromise: promisify(delGroup),
  };
}

module.exports = store;
