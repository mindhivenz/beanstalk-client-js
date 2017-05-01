import FiveBeansClient from 'fivebeans/lib/client'


export default class BeanstalkDriver {

  _client = null

  constructor({ host = '127.0.0.1', port, tube }) {
    if (! port) {
      throw new Error('Be explicit about a test port, you should not use the standard port / beanstalkd instance')
    }
    this.host = host
    this.port = port
    this.tube = tube
  }

  get client() {
    if (! this._client) {
      throw new Error('mochaHooks() not called')
    }
    return this._client
  }

  mochaHooks() {
    global.before(this._before)
    global.beforeEach(this._beforeEach)
    global.after(this._after)
  }

  async outstandingJobCount() {
    return new Promise((resolve, reject) => {
      this._client.stats((statsErr, response) => {
        if (statsErr) {
          reject(statsErr)
        } else {
          resolve(response['current-jobs-ready'] + response['current-jobs-reserved'])
        }
      })
    })
  }

  async givenPayloadPut(
    payload,
    {
      priority = 0,
      delay = 0,
      ttr = 1,
      jsonPayload = true,
    } = {}) {
    await new Promise((resolve, reject) => {
      this._client.put(priority, delay, ttr, jsonPayload ? JSON.stringify(payload) : payload, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  _before = (done) => {
    this._client = new FiveBeansClient(this.host, this.port)
    this._client
      .on('error', done)
      .on('connect', () => {
        if (this.tube) {
          this._client.use(this.tube, done)
        } else {
          done()
        }
      })
      .connect()
  }

  _beforeEach = (done) => {

    const deleteOneJob = async () => {
      if (await this.outstandingJobCount()) {
        this._client.reserve((reserveErr, jobid) => {
          if (reserveErr) {
            done(reserveErr)
          } else {
            this._client.destroy(jobid, (destoryErr) => {
              if (destoryErr) {
                done(destoryErr)
              } else {
                deleteOneJob()
              }
            })
          }
        })
      } else {
        done()
      }
    }

    deleteOneJob()
  }


  _after = () => {
    this._client.quit()
  }
}
