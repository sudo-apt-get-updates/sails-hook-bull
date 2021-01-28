var path = require('path');
var fs = require('fs');
var _ = require('lodash')

var Queue = require('bull');

module.exports = (sails) => {
  return {
    initialize: async function () {
      let queueDir = path.join(sails.config.appPath, 'api/queues')
      if (!fs.existsSync(queueDir)) {
        return;
      }

      let queueFiles = fs.readdirSync(queueDir).filter(fn => /\.js$/.test(fn))

      for (let file of queueFiles) {
        let filePath = path.join(queueDir, file)

        let obj = buildQueue.bind(this)(sails, filePath);
        if (obj == null) {
          continue;
        }
        let queue = obj.queue;
        let queueData = obj.queueData;

        if (queue == null) {
          continue;
        }

        if (queue.name == null) {
          continue;
        }

        let queueFile = path.basename(filePath, '.js')

        //Make queue accesible
        this[queueFile] = queue;
        if (queueData.startup) {
          if (queueData.opts) {
            this[queueFile].add(`${queueData.queueName}-process`, {}, queueData.opts);
          }
        }

      }
    }
  }
};


const buildQueue = function (sails, filePath) {
  let queueData = getDataFromPath.bind(this)(sails, filePath)

  if (queueData == null) {
    return null;
  }

  let queue = createQueue(queueData)

  if (queue == null) {
    return null;
  }

  //Create process if available
  if ('process' in queueData) {
    createProcess(queue, queueData)
  }

  return {queue: queue, queueData: queueData};
}

const getDataFromPath = function (sails, filePath) {
  let queueData = require(filePath)
  getDataFromPath
  let isObject = queueData !== null && typeof queueData === 'object'
  if (!isObject) {
    return null;
  }

  // Merge queue options with config/bull.js
  Object.assign(queueData, {opts: _.assign(sails.config[this.configKey], queueData.opts)})

  return queueData;
}


/**
 * Create queue from file definition
 */
const createQueue = function (queueData) {

  if (queueData == null) {
    return null;
  }

  if (queueData.queueName == null) {
    return null;
  }

  //Pick args in order
  let args = _.pick(queueData, ['queueName', 'url', 'opts', 'startup'])
  args = Object.values(args)

  return Queue(...args)
}


/**
 * Create process for a queue
 */
const createProcess = (queue, queueData) => {
  if (typeof queueData.process === "function") {
    queue.process(queueData.process)
  } else {
    let args = _.pick(queueData.process, ['name', 'concurrency', 'processor'])
    args = Object.values(args)

    queue.process(...args)
  }
}
