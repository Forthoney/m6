// @ts-check

const assert = require("assert");
const crypto = require("crypto");
const serialization = require("./serialization");

/**
 * @typedef {!string} ID
 */

/**
 * The ID is the SHA256 hash of the JSON representation of the object
 * @param {any} obj
 * @returns {ID}
 */
function getID(obj) {
  const hash = crypto.createHash("sha256");
  hash.update(serialization.serialize(obj));
  return hash.digest("hex");
}

/**
 * The NID is the SHA256 hash of the JSON representation of the node
 * @param {object} node
 * @returns {ID}
 */
function getNID(node) {
  node = { ip: node.ip, port: node.port };
  return getID(node);
}

/**
 * The SID is the first 5 characters of the NID
 * @param {object} node
 * @returns {ID}
 */
function getSID(node) {
  return getNID(node).substring(0, 5);
}

/**
 * Converts ID to number
 * @param {ID} id
 * @returns {number}
 */
function idToNum(id) {
  let n = parseInt(id, 16);
  assert(!isNaN(n), "idToNum: id is not in KID form!");
  return n;
}

/**
 * @param {ID} kid
 * @param {ID[]} nids
 * @returns {ID}
 */
function naiveHash(kid, nids) {
  nids.sort();
  return nids[idToNum(kid) % nids.length];
}

/**
 * @param {number} numKID
 * @param {number[]} numNIDs
 * @returns {number}
 */
function findDestinationNum(numNIDs, numKID) {
  return numNIDs.find((nNID) => nNID >= numKID) || numNIDs[0];
}

/**
 * @param {ID} kid
 * @param {ID[]} nids
 * @returns {ID}
 */
function consistentHash(kid, nids) {
  const numNIDMap = new Map(nids.map((nid) => [idToNum(nid), nid]));
  const numNIDs = Array.from(numNIDMap.keys());
  numNIDs.sort((a, b) => a - b);
  return numNIDMap[findDestinationNum(numNIDs, idToNum(kid))];
}

/**
 * @param {ID} kid
 * @param {ID[]} nids
 * @returns {ID}
 */
function rendezvousHash(kid, nids) {
  const combinedNumID = nids.map((nid) => idToNum(getID(kid + nid)));
  const idxOfMaxNumID = combinedNumID.reduce(
    (maxIndex, elem, i, arr) => (elem > arr[maxIndex] ? i : maxIndex),
    0,
  );
  return nids[idxOfMaxNumID];
}

module.exports = {
  getNID: getNID,
  getSID: getSID,
  getID: getID,
  idToNum: idToNum,
  naiveHash: naiveHash,
  consistentHash: consistentHash,
  rendezvousHash: rendezvousHash,
};
