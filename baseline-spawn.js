const path = require("node:path");
const { fork } = require("node:child_process");
const http = require("http");
const { exit } = require("node:process");

const distPath = path.join(__dirname, "./baseline-spawn.js");

function forkBlank() {
  return new Promise((resolve, reject) => {
    const child = fork(distPath);
    child.on("error", () => reject());
    child.on("message", () => resolve(child));
  });
}

if (process.send === undefined) {
  const obs = new PerformanceObserver((items) => {
    console.log(items);
    performance.clearMarks();
  });
  obs.observe({ type: "measure" });
  performance.mark("init");
  const children = Array.from({ length: parseInt(process.argv[2]) }).map(() =>
    forkBlank(),
  );
  Promise.all(children).then((children) => {
    console.log("here");
    performance.measure("All children spawned", "init");
    children.forEach((c) => c.kill());
  });
} else {
  http
    .createServer((req, res) => {})
    .listen(() => {
      process.send("spawned");
    });
}
