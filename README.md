## Usage
### Frontend
To deploy the frontend application, run npm install in the /app directory. Run `npm start` to start the frontend server. The application runs on port 3000, so visit http://localhost:3000 or the public IP address of a deployed server to access the GUI.

### Framework
To run the Boogle framework, clone the repository. Run `./distribution.js` from the root of the repository. By default, this will create a process that listens on 127.0.0.1:8080. This can be modified by setting the `--ip` and `--port` option. To launch multiple processes locally, use the `--local [NUMBER]` to launch NUMBER processes. There exists a `--aws` flag, but this is currently hardcoded to point to our AWS EC2 instances' public IP address, so user intervention is necessary before using this flag. The crawl and index workflows can be triggered with the `--crawl` and `--index` flag respectively.
