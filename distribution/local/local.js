/*

Service  Description                                Methods
status   Status and control of the current node     get, spawn, stop
comm     A message communication interface          send
groups   A mapping from group names to nodes        get, put, add, rem, del
gossip   The receiver part of the gossip protocol   recv
routes   A mapping from names to functions          get, put

*/

const status = require("./status");
const groups = require("./groups");
const routes = require("./routes");
const comm = require("./comm");
const gossip = require("./gossip");
const mem = require("./mem");
const store = require("./store");

module.exports = {
  status: status,
  routes: routes,
  comm: comm,
  groups: groups,
  gossip: gossip,
  mem: mem,
  store: store,
};
