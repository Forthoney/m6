const local = require("../local/local");

function map(keys, mapper) {
  const mapperResults = Map();
  keys.forEach((key) => {
    local.store.get(key, (e, val) => {
      if (e) throw e;

      const res = mapper(key, val);
      Object.entries(res).forEach((mKey, mVal) => {
        if (mapperResults.has(mKey)) {
          mapperResults.get(mKey).push(mVal);
        } else {
          mapperResults.set(mKey, [mVal]);
        }
      });
    });
  });

  return mapperResults;
}

function reduce(mapResults, reducer) {
  const reducerResults = Map();
  mapResults.forEach((k, v) => {
    reducer(k, v);
  });
}

function mr(config) {
  const context = {};
  context.gid = config.gid || "all";

  return {
    exec: (setting, callback = () => {}) => {
      /* Change this with your own exciting Map Reduce code! */
      try {
        const mapResults = map(setting.keys, setting.mapper);
        mapResults.forEach((k, v) => {});
      } catch (e) {
        callback(e);
      }
    },
  };
}

module.exports = mr;
