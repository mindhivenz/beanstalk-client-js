import FiveBeansClient from 'fivebeans/lib/client'


export const DEFAULT_TUBE = 'default'

class Job {

  constructor(id, rawPayload, _client) {
    this.id = id
    this.payload = JSON.parse(rawPayload)
    this._client = _client
  }

  async done() {
    if (! this._markedDone) {
      await new Promise((resolve, reject) => {
        this._client.destroy(this.id, (err) => {
          if (err) {
            reject(err)
          } else {
            this._markedDone = true
            resolve()
          }
        })
      })
    }
  }

}

export default class BeanstalkClient {

  constructor({
    host = '127.0.0.1',
    port = 11300,
    tube = DEFAULT_TUBE,
  } = {}) {
    this._client = new FiveBeansClient(host, port)
    this.tube = tube
    this.connected = new Promise((resolve, reject) => {
      this._client
        .on('connect', () => {
          if (tube === DEFAULT_TUBE) {
            resolve()
          } else {
            this._client.watch(tube, (watchErr) => {
              if (watchErr) {
                reject(new Error(`Beanstalk watch ${tube} failed: ${watchErr}`))
              } else {
                this._client.ignore(DEFAULT_TUBE, (ignoreErr) => {
                  if (ignoreErr) {
                    reject(new Error(`Beanstalk ignore ${DEFAULT_TUBE} failed: ${ignoreErr}`))
                  } else {
                    resolve()
                  }
                })
              }
            })
          }
        })
        .on('error', (errString) => {
          reject(new Error(`Beanstalk error ${errString}`))
        })
        .on('close', () => {
          // We could reconnect here but assume that process will exit and be restarted by systemd or pm2 or similar
          this.connected = Promise.reject(new Error('Connection closed'))
        })
        .connect()
    })
  }

  async reserve() {
    await this.connected
    return await new Promise((resolve, reject) => {
      this._client.reserve((err, id, rawPayload) => {
        if (err) {
          reject(err)
        } else {
          try {
            resolve(new Job(id, rawPayload, this._client))
          } catch (e) {
            reject(e)
          }
        }
      })
    })
  }

  async processLoop(asyncJobHandler) {
    // noinspection InfiniteLoopJS
    for (;;) {
      const job = await this.reserve()
      // REVISIT: should we make this asynchronous (handle jobs in parallel?)
      await asyncJobHandler(job)
      await job.done()
    }
  }

}
