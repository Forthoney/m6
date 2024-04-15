// workaround for no class eslint rule
const typedef = Symbol("typedef");

function referenceConstructor(ref) {
  return { [typedef]: "Reference", id: ref };
}

function objectIdConstructor(oid) {
  return { [typedef]: "ObjectId", id: oid };
}

function idMapConstructor() {
  const instance = {
    [typedef]: "IdMap",
    superclass: new Map(),
    id: 0,
    register: (obj, self = instance) => {
      self.superclass.set(obj, self.id);
      self.id += 1;
      return self.id - 1;
    },
    has: (obj, self = instance) => {
      return self.superclass.has(obj);
    },
    get: (obj, self = instance) => {
      return self.superclass.get(obj);
    },
  };
  return instance;
}

function bidirectionalIdMap() {
  const instance = {
    [typedef]: "BidirectionalIdMap",
    objToId: idMapConstructor(),
    idToObj: new Map(),
    register: (obj, self = instance) => {
      self.idToObj.set(self.objToId.register(obj), obj);
    },
    hasObj: (obj, self = instance) => {
      return self.objToId.has(obj);
    },
    getFromObj: (obj, self = instance) => {
      return self.objToId.get(obj);
    },
    getFromId: (id, self = instance) => {
      return self.idToObj.get(id);
    },
  };
  return instance;
}

function createGlobalIndex() {
  const toCheck = [
    "fs",
    "http",
    "https",
    "url",
    "path",
    "os",
    "events",
    "stream",
    "util",
    "querystring",
    "zlib",
    "buffer",
    "child_process",
    "cluster",
    "dgram",
    "dns",
    "http2",
    "v8",
  ].map(require);
  toCheck.push(globalThis);

  const bidirectionalMap = bidirectionalIdMap();

  while (toCheck.length > 0) {
    const elem = toCheck.shift();
    bidirectionalMap.register(elem);
    if (elem != null && typeof elem === "object") {
      Reflect.ownKeys(elem).forEach((key) => {
        const child = elem[key];
        if (!bidirectionalMap.hasObj(child)) {
          toCheck.push(child);
        }
      });
    }
  }

  return bidirectionalMap;
}

const GLOBAL_INDEX = createGlobalIndex();

/*
 * Serialize given object. Adds a ROOT wrapper around the raw object
 */
function serialize(object) {
  const idMap = idMapConstructor();
  const res = `{"type": "ROOT", "value": ${serializeInner(object, idMap)}}`;
  return res;
}

/*
 * Recursively serialize object without the wrapper.
 */
function serializeInner(object, mapping) {
  if (object === null) {
    return "null";
  }

  if (object instanceof Buffer) {
    console.error(object);
  }
  switch (typeof object) {
    case "number":
    case "boolean":
      return object.toString();
    case "string":
      return JSON.stringify(object);
    case "undefined":
      return '{"type": "undefined", "value": "undefined"}';
    case "function":
      return GLOBAL_INDEX.hasObj(object)
        ? `{"type": "builtin", "value": ${GLOBAL_INDEX.getFromObj(object)}}`
        : `{"type": "function", "value": ${JSON.stringify(object.toString())}}`;
    case "object":
      return mapping.has(object)
        ? `{"type": "Reference", "value": "${mapping.get(object)}"}`
        : serializeComplex(object, mapping);
    default:
      throw Error("Unsupported type");
  }
}

function serializeArray(array, mapping) {
  const objectId = mapping.register(array);
  const stringifiedArr = array.map((elem) => serializeInner(elem, mapping));
  const objectIdMetadata = `{"type": "ObjectId", "value": ${objectId}}`;
  stringifiedArr.push(objectIdMetadata);
  return `[${stringifiedArr.join(",")}]`;
}

function serializeObject(object, mapping) {
  const objectId = mapping.register(object);
  const entries = Object.entries(object).map((entry) =>
    serializeKeyValue(...entry, mapping),
  );
  entries.push(`"objectId": {"type": "ObjectId", "value": ${objectId}}`);
  return `{${entries.join(",")}}`;
}

/*
 * Keywords "type" "value" "objectId" should not be used. Therefore, if
 * a key is named "type" or "value", prepend a underscore.
 * This means we may have name clashes with "_type", "_value", "__type", and
 * so on. So, we prepend "_" to all keys with pattern.
 */
function serializeKeyValue(key, value, mapping) {
  const match = key.match(/(^_*type$)|(^_*value$)|(^_*objectId$)/g);
  if (match === null) {
    return `"${key}": ${serializeInner(value, mapping)}`;
  } else {
    return `"_${key}": ${serializeInner(value, mapping)}`;
  }
}

/*
 * Serialize complex objects. This includes Object, Array, Date, etc
 */
function serializeComplex(object, mapping) {
  if (object instanceof Array) {
    return serializeArray(object, mapping);
  } else if (object instanceof Date) {
    return `{"type": "Date", "value": ${JSON.stringify(object)}}`;
  } else if (object instanceof Error) {
    return `{"type": "Error", "value": ${JSON.stringify(object.message)}}`;
  } else {
    return serializeObject(object, mapping);
  }
}

function parseWrapper(item) {
  switch (item.type) {
    case "boolean":
    case "number":
      return JSON.parse(item.value);
    case "string":
    case "ROOT":
      return item.value;
    case "undefined":
      return undefined;
    case "function":
      return new Function("require", `return ${item.value}`)(require);
    case "builtin":
      return GLOBAL_INDEX.getFromId(JSON.parse(item.value));
    case "Date":
      return new Date(item.value);
    case "Error":
      return Error(item.value);
    case "Reference":
      return referenceConstructor(JSON.parse(item.value));
    case "ObjectId":
      return objectIdConstructor(JSON.parse(item.value));
    default:
      throw Error(`unexpected type: ${item.type}`);
  }
}

function createObjectIdMap(obj, objectIdMap) {
  Object.values(obj).forEach((item) => {
    if (item == null) return;
    if (item[typedef] === "ObjectId") {
      objectIdMap.set(item.id, obj);
      if (obj instanceof Array) {
        obj.pop();
      } else {
        delete obj.objectId;
      }
    } else if (typeof item === "object") {
      createObjectIdMap(item, objectIdMap);
    }
  });
}

function replaceReferences(object, objectIdMap) {
  Object.entries(object).forEach((entry) => {
    const [prop, item] = entry;
    if (item == null) return;

    if (item[typedef] === "Reference") {
      const referencedObject = objectIdMap.get(item.id);
      if (referencedObject != null) {
        // Replace the Reference with the corresponding object from objectIdMap
        object[prop] = referencedObject;
      } else {
        throw Error("Object ID not found");
      }
    } else if (item instanceof Object) {
      // If the current property is an object, recursively call the function
      replaceReferences(item, objectIdMap);
    }
  });
}

function removeLeadingUnderscore(obj) {
  if (obj && typeof obj === "object") {
    if (Array.isArray(obj)) {
      obj.forEach((element, index) => {
        obj[index] = removeLeadingUnderscore(element);
      });
    } else {
      const newObj = {};
      Object.keys(obj).forEach((key) => {
        if (key.match(/(^_+type$)|(^_+value$)|(^_+objectId$)/g)) {
          const newKey = key.replace(/^_/, "");
          newObj[newKey] = removeLeadingUnderscore(obj[key]);
        } else {
          newObj[key] = removeLeadingUnderscore(obj[key]);
        }
      });
      return newObj;
    }
  }

  return obj;
}

function deserialize(string) {
  // flag if any keyword replacement occurred
  let removeUnderscore = false;

  if (string == "") {
    console.error("EMPTY STRING=================================");
  }
  const parsed = JSON.parse(string, (key, item) => {
    removeUnderscore =
      removeUnderscore || key.match(/(^_+type$)|(^_+value$)|(^_+objectId$)/g);
    return item instanceof Object && "type" in item ? parseWrapper(item) : item;
  });
  const objectIdMap = new Map();
  if (parsed instanceof Object) {
    createObjectIdMap(parsed, objectIdMap);
    replaceReferences(parsed, objectIdMap);
  }
  // if removeUnderscore flag was not set, recursive traversal to replace keys
  // is skipped
  return removeUnderscore ? removeLeadingUnderscore(parsed) : parsed;
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};
