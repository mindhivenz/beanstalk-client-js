import FiveBeansClient from 'fivebeans/lib/client'
import { app } from '@mindhive/di'


export const DEFAULT_TUBE = 'default'

export default class BeanstalkClient {

  log = app().log

  constructor({
    host = '127.0.0.1',
    port = 11300,
    tube = DEFAULT_TUBE,
  } = {}) {
    this.client = new FiveBeansClient(host, port)
    this.connected = new Promise((resolve, reject) => {
      this.client
        .on('connect', () => {
          if (tube === DEFAULT_TUBE) {
            resolve()
          } else {
            this.client.watch(tube, (watchErr) => {
              if (watchErr) {
                reject(new Error(`Beanstalk watch failed: ${watchErr}`))
              } else {
                this.client.ignore(DEFAULT_TUBE, (ignoreErr) => {
                  this.log.warn(`Beanstalk ignore ${DEFAULT_TUBE} tube failed: ${ignoreErr}`)
                })
                resolve()
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
      this.client.reserve((err, ...args) => {
        if (err) {
          reject(err)
        } else {
          try {
            resolve(this._createJob(...args))
          } catch (e) {
            reject(e)
          }
        }
      })
    })
  }

  _createJob(id, payload) {
    const done = async () => {
      await new Promise((resolve) => {
        this.client.destroy(id, (err) => {
          if (err) {
            this.log.warn(`Failed to destroy job ${id}: ${err}`)
          }
          resolve()
        })
      })
    }
    return {
      id,
      payload: JSON.parse(payload),
      done,
    }
  }

}
