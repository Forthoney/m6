global.nodeConfig = {ip: '127.0.0.1', port: 8080};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

// This group is used for testing most of the functionality
const mygroupGroup = {};
// These groups are used for testing hashing
const group1Group = {};
const group2Group = {};
const group3Group = {};
// This group is used for {adding,removing} {groups,nodes}
const group4Group = {};


/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   This is because the process that node is
   running in is the actual jest process
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 8000};
const n2 = {ip: '127.0.0.1', port: 8001};
const n3 = {ip: '127.0.0.1', port: 8002};
const n4 = {ip: '127.0.0.1', port: 8003};
const n5 = {ip: '127.0.0.1', port: 8004};
const n6 = {ip: '127.0.0.1', port: 8005};

beforeAll((done) => {
  // First, stop the nodes if they are running
  let remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
            });
          });
        });
      });
    });
  });

  mygroupGroup[id.getSID(n1)] = n1;
  mygroupGroup[id.getSID(n2)] = n2;
  mygroupGroup[id.getSID(n3)] = n3;

  group1Group[id.getSID(n4)] = n4;
  group1Group[id.getSID(n5)] = n5;
  group1Group[id.getSID(n6)] = n6;

  group2Group[id.getSID(n1)] = n1;
  group2Group[id.getSID(n3)] = n3;
  group2Group[id.getSID(n5)] = n5;

  group3Group[id.getSID(n2)] = n2;
  group3Group[id.getSID(n4)] = n4;
  group3Group[id.getSID(n6)] = n6;

  group4Group[id.getSID(n1)] = n1;
  group4Group[id.getSID(n2)] = n2;
  group4Group[id.getSID(n4)] = n4;

  // Now, start the base listening node
  distribution.node.start((server) => {
    localServer = server;

    const groupInstantiation = (e, v) => {
      const mygroupConfig = {gid: 'mygroup'};
      const group1Config = {gid: 'group1', hash: id.naiveHash};
      const group2Config = {gid: 'group2', hash: id.consistentHash};
      const group3Config = {gid: 'group3', hash: id.rendezvousHash};
      const group4Config = {gid: 'group4'};

      // Create some groups
      groupsTemplate(mygroupConfig)
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            groupsTemplate(group1Config)
                .put(group1Config, group1Group, (e, v) => {
                  groupsTemplate(group2Config)
                      .put(group2Config, group2Group, (e, v) => {
                        groupsTemplate(group3Config)
                            .put(group3Config, group3Group, (e, v) => {
                              groupsTemplate(group4Config)
                                  .put(group4Config, group4Group, (e, v) => {
                                    done();
                                  });
                            });
                      });
                });
          });
    };

    // Start the nodes
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          distribution.local.status.spawn(n4, (e, v) => {
            distribution.local.status.spawn(n5, (e, v) => {
              distribution.local.status.spawn(n6, groupInstantiation);
            });
          });
        });
      });
    });
  });
});

afterAll((done) => {
  distribution.mygroup.status.stop((e, v) => {
    let remote = {service: 'status', method: 'stop'};
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n3;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n4;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n5;
            distribution.local.comm.send([], remote, (e, v) => {
              remote.node = n6;
              distribution.local.comm.send([], remote, (e, v) => {
                localServer.close();
                done();
              });
            });
          });
        });
      });
    });
  });
});

test('(2 pts) all.store.reconf(naiveHash)', (done) => {
  //  ________________________________________
  // / NOTE: If this test fails locally, make \
  // | sure you delete the contents of the    |
  // | store/ directory (not the directory    |
  // | itself!), so your results are          |
  // \ reproducible                           /
  //  ----------------------------------------
  //         \   ^__^
  //          \  (oo)\_______
  //             (__)\       )\/\
  //                 ||----w |
  //                 ||     ||

  // group1 - naiveHash - n4, n5, n6

  // First, we check where the keys should be placed
  // before we change the group's nodes.
  // group1 uses the naiveHash function for item placement,
  // so we test using the same naiveHash function
  const users = [
    {first: 'Emma', last: 'Watson'},
    {first: 'John', last: 'Krasinski'},
    {first: 'Julie', last: 'Bowen'},
    {first: 'Sasha', last: 'Spielberg'},
    {first: 'Tim', last: 'Nelson'},
  ];
  const keys = [
    'ewatsonmrnh',
    'jkrasinskimrnh',
    'jbowenmrnh',
    'sspielbergmrnh',
    'tnelsonmrnh',
  ];
  const kids = keys.map((key) => id.getID(key));
  const nodes = [n4, n5, n6];
  const nids = nodes.map((node) => id.getNID(node));

  const nidsPicked = kids.map((kid) => id.naiveHash(kid, nids));
  const nodesPicked = nidsPicked.map(
      (nid) => nodes.filter((node) => id.getNID(node) === nid)[0],
  );
  // key 0 ends up on n6, while keys 1-4 end up on n4
  // (the following console.logs should confirm that)
  nodesPicked.forEach(
      (node, key) => console.log('BEFORE! key: ', key, 'node: ', node),
  );

  // Then, we remove n5 from the list of nodes,
  // and use the naiveHash function again,
  // to see where items should end up after this change
  const nodesAfter = [n4, n6];
  const nidsAfter = nodesAfter.map((node) => id.getNID(node));

  const nidsPickedAfter = kids.map((kid) => id.naiveHash(kid, nidsAfter));
  const nodesPickedAfter = nidsPickedAfter.map(
      (nid) => nodesAfter.filter((node) => id.getNID(node) === nid)[0],
  );

  // After removal, all keys end up on n6
  // (Again, the console.logs should be consistent with that!)
  nodesPickedAfter.forEach(
      (node, key) => console.log('AFTER! key: ', key, 'node: ', node),
  );

  // This function will be called after we put items in nodes
  const checkPlacement = (e, v) => {
    try {
      const remote = {node: n6, service: 'store', method: 'get'};
      const messages = [
        [{key: keys[0], gid: 'group1'}],
        [{key: keys[1], gid: 'group1'}],
        [{key: keys[2], gid: 'group1'}],
        [{key: keys[3], gid: 'group1'}],
        [{key: keys[4], gid: 'group1'}],
      ];

      distribution.local.comm.send(messages[0], remote, (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v).toEqual(users[0]);
        } catch (error) {
          done(error);
        }

        distribution.local.comm.send(messages[1], remote, (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v).toEqual(users[1]);
          } catch (error) {
            done(error);
          }

          distribution.local.comm.send(messages[2], remote, (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v).toEqual(users[2]);
            } catch (error) {
              done(error);
            }

            distribution.local.comm.send(messages[3], remote, (e, v) => {
              try {
                expect(e).toBeFalsy();
                expect(v).toEqual(users[3]);
              } catch (error) {
                done(error);
              }

              distribution.local.comm.send(messages[4], remote, (e, v) => {
                try {
                  expect(e).toBeFalsy();
                  expect(v).toEqual(users[4]);
                  distribution["group1"].store.get(null, (err, allKeys) => {
                    console.log("IN TEST", allKeys);
                  });
                  done();
                } catch (error) {
                  done(error);
                }
              });
            });
          });
        });
      });
    } catch (error) {
      done(error);
    }
  };

  // Now we actually put items in the group,
  // remove n5, and check if the items are placed correctly
  distribution.group1.store.put(users[0], keys[0], (e, v) => {
    distribution.group1.store.put(users[1], keys[1], (e, v) => {
      distribution.group1.store.put(users[2], keys[2], (e, v) => {
        distribution.group1.store.put(users[3], keys[3], (e, v) => {
          distribution.group1.store.put(users[4], keys[4], (e, v)=> {
            // We need to pass a copy of the group's
            // nodes before the changes to reconf()
            const groupCopy = {...group1Group};
            distribution.group1.groups.rem(
                'group1',
                id.getSID(n5),
                (e, v) => {
                  distribution.group1.store.reconf(groupCopy, checkPlacement);
                });
          });
        });
      });
    });
  });
});

// test('(2 pts) all.mem.reconf(naiveHash)', (done) => {
//   // group1 - naiveHash - n4, n5, n6

//   // First, we check where the keys should be placed
//   // before we change the group's nodes.
//   // group1 uses the naiveHash function for item placement,
//   // so we test using the same naiveHash function
//   const users = [
//     {first: 'Emma', last: 'Watson'},
//     {first: 'John', last: 'Krasinski'},
//     {first: 'Julie', last: 'Bowen'},
//     {first: 'Sasha', last: 'Spielberg'},
//     {first: 'Tim', last: 'Nelson'},
//   ];
//   const keys = [
//     'ewatsonmrnh',
//     'jkrasinskimrnh',
//     'jbowenmrnh',
//     'sspielbergmrnh',
//     'tnelsonmrnh',
//   ];
//   const kids = keys.map((key) => id.getID(key));
//   const nodes = [n4, n5, n6];
//   const nids = nodes.map((node) => id.getNID(node));

//   const nidsPicked = kids.map((kid) => id.naiveHash(kid, nids));
//   const nodesPicked = nidsPicked.map(
//       (nid) => nodes.filter((node) => id.getNID(node) === nid)[0],
//   );
//   // key 0 ends up on n6, while keys 1-4 end up on n4
//   // (the following console.logs should confirm that)
//   nodesPicked.forEach(
//       (node, key) => console.log('BEFORE! key: ', key, 'node: ', node),
//   );

//   // Then, we remove n5 from the list of nodes,
//   // and use the naiveHash function again,
//   // to see where items should end up after this change
//   const nodesAfter = [n4, n6];
//   const nidsAfter = nodesAfter.map((node) => id.getNID(node));

//   const nidsPickedAfter = kids.map((kid) => id.naiveHash(kid, nidsAfter));
//   const nodesPickedAfter = nidsPickedAfter.map(
//       (nid) => nodesAfter.filter((node) => id.getNID(node) === nid)[0],
//   );

//   // After removal, all keys end up on n6
//   // (Again, the console.logs should be consistent with that!)
//   nodesPickedAfter.forEach(
//       (node, key) => console.log('AFTER! key: ', key, 'node: ', node),
//   );

//   // This function will be called after we put items in nodes
//   const checkPlacement = (e, v) => {
//     try {
//       const remote = {node: n6, service: 'mem', method: 'get'};
//       const messages = [
//         [{key: keys[0], gid: 'group1'}],
//         [{key: keys[1], gid: 'group1'}],
//         [{key: keys[2], gid: 'group1'}],
//         [{key: keys[3], gid: 'group1'}],
//         [{key: keys[4], gid: 'group1'}],
//       ];

//       distribution.local.comm.send(messages[0], remote, (e, v) => {
//         try {
//           expect(e).toBeFalsy();
//           expect(v).toEqual(users[0]);
//         } catch (error) {
//           done(error);
//         }

//         distribution.local.comm.send(messages[1], remote, (e, v) => {
//           try {
//             expect(e).toBeFalsy();
//             expect(v).toEqual(users[1]);
//           } catch (error) {
//             done(error);
//           }

//           distribution.local.comm.send(messages[2], remote, (e, v) => {
//             try {
//               expect(e).toBeFalsy();
//               expect(v).toEqual(users[2]);
//             } catch (error) {
//               done(error);
//             }

//             distribution.local.comm.send(messages[3], remote, (e, v) => {
//               try {
//                 expect(e).toBeFalsy();
//                 expect(v).toEqual(users[3]);
//               } catch (error) {
//                 done(error);
//               }

//               distribution.local.comm.send(messages[4], remote, (e, v) => {
//                 try {
//                   expect(e).toBeFalsy();
//                   expect(v).toEqual(users[4]);
//                   done();
//                 } catch (error) {
//                   done(error);
//                 }
//               });
//             });
//           });
//         });
//       });
//     } catch (error) {
//       done(error);
//     }
//   };

//   // Now we actually put items in the group,
//   // remove n5, and check if the items are placed correctly
//   distribution.group1.mem.put(users[0], keys[0], (e, v) => {
//     distribution.group1.mem.put(users[1], keys[1], (e, v) => {
//       distribution.group1.mem.put(users[2], keys[2], (e, v) => {
//         distribution.group1.mem.put(users[3], keys[3], (e, v) => {
//           distribution.group1.mem.put(users[4], keys[4], (e, v)=> {
//             // We need to pass a copy of the group's
//             // nodes before the changes to reconf()
//             const groupCopy = {...group1Group};
//             distribution.group1.groups.rem(
//                 'group1',
//                 id.getSID(n5),
//                 (e, v) => {
//                   distribution.group1.mem.reconf(groupCopy, checkPlacement);
//                 });
//           });
//         });
//       });
//     });
//   });
// });
