require('dotenv').config()
const jq = require('bigjq')
const mingo = require('mingo')
const MongoClient = require('mongodb').MongoClient
const traverse = require('traverse')
var collectionNames
var transform
const ops = ["db", "find", "aggregate", "sort", "project", "limit", "skip", "distinct", "count"]
var sort
var limit
var validate = function(r) {
  if (typeof r.v === 'undefined') {
    return { status: "invalid", result: false, errors: ["v missing"] }
  }
  if (typeof r.q === 'undefined') {
    return { status: "invalid", result: false, errors: ['q missing'] }
  }
  let keys = Object.keys(r.q)
  if (keys.length === 0) {
    return { status: "invalid", result: false, errors: ['q empty'] }
  }
  let errors = []
  for (let i=0; i<keys.length; i++) {
    if (ops.indexOf(keys[i]) < 0) {
      errors.push("invalid MongoDB op(supported: find, aggregate, sort, project, limit, distinct, count)")
      return { status: "invalid", result: false, errors: errors }
    }
  }
  return { status: "valid", result: true }
}
var read = async function(address, r, debug) {
  let isvalid = validate(r)
  if (!isvalid.result) return isvalid;

  let result = {}
  // 1. v: version
  // 2. q: query
  // 3. r: response
  if (r.q) {
    // "encode": transform the query into stored data format
    let query = (transform && transform.request) ? transform.request(r.q) : r.q
    let promises = []
    let src = (query.db && query.db.length > 0) ? query.db : Manager[address].collectionNames
    for (let i=0; i<src.length; i++) {
      let key = src[i];
      if (src.indexOf(key) >= 0) {
        promises.push(lookup(address, query, key, r.r, debug))
      }
    }

    try {
      let responses = await Promise.all(promises)
      responses.forEach(function(response) {
        result[response.name] = response.items
      })
    } catch (e) {
      if (result.errors) {
        result.errors.push(e.toString())
      } else {
        result.errors = [e.toString()]
      }
    }
  }
  return result
}
var exit = function() {
  for(let key in Manager) {
    Manager[key].client.close()
  }
}
var Manager = {}
var tryInit = function(config, cb) {
  let url = (config && config.url ? config.url : "mongodb://localhost:27017")
  let address = config.address
  sort = config.sort
  limit = config.limit
  let sockTimeout = (config && config.timeout) ? config.timeout + 100 : 20100
  console.log("Connecting to DB...")
  MongoClient.connect(url, {
    useNewUrlParser: true,
    socketTimeoutMS: sockTimeout
  }, function(err, _client) {
    if (err) {
      console.log("Not ready, trying again in 1 second...")
      setTimeout(function() {
        tryInit(config, cb)
      }, 1000)
    } else {
      console.log("DB Online!")
      let _db = _client.db(address)

      _db.listCollections().toArray(function(err, infos) {
        let collectionNames = infos.map(function(info) { return info.name })
        console.log("Collection Names = ", collectionNames)
        if (collectionNames && collectionNames.length > 0) {
          console.log("Collections exist!")
          cb(_client, _db)
        } else {
          console.log("Collections don't exist yet. Retrying after 1 second...")
          _client.close()
          setTimeout(function() {
            tryInit(config, cb)
          }, 1000)
        }
      })

      //cb(_client, _db)
    }
  })
}
var init = function(config) {
  console.log("Bitquery init", config)
  transform = config.transform
  return new Promise(function(resolve, reject) {
    let url = (config && config.url ? config.url : "mongodb://localhost:27017")
    let address = config.address
    if (/mongodb:.*/.test(url)) {
      tryInit(config, function(_client, _db) {
        Manager[address] = {
          client: _client,
          db: _db
        }
        if (config && config.timeout) {
          Manager[address].timeout = config.timeout
        }
        Manager[address].db.listCollections().toArray(function(err, infos) {
          collectionNames = infos.map(function(info) { return info.name })
          console.log("Collection Names = ", collectionNames)
          Manager[address].collectionNames = collectionNames
          resolve({ _db: _db, read: read, exit: exit })
        })
      })
    } else {
      reject("Invalid Node URL")
    }
  })
}
//var filter = {
//  /*
//  event := {
//    message: [full event payload],
//    query: [bitquery filter]
//  }
//  */
//  block: async function(event) {
//    let message = event.message
//    // encode the query to match the stored data format
//    //const encoded = bcode.encode(event.query)
//    const encoded = transform.request(event.query)
//    let _filter = new mingo.Query(encoded.q.find)
//    let items = message.items
//    let _filtered = items.filter(function(e) {
//      return _filter.test(e)
//    })
//    let transformed = []
//    for(let i=0; i<_filtered.length; i++) {
//      let tx = _filtered[i]
//      // the stored data has returned.
//      // transform the stored transaction into user-facing format
//      //let decoded = bcode.decode(tx)
//      let decoded = transform.response(tx)
//      let result
//      try {
//        if (encoded.r && encoded.r.f) {
//          result = await jq.run(encoded.r.f, [decoded])
//        } else {
//          result = decoded
//        }
//        transformed.push(result)
//      } catch (e) {
//        console.log("Error", e)
//      }
//    }
//    return transformed
//  },
//  mempool: async function(event) {
//    let message = event.message
//    //const encoded = bcode.encode(event.query)
//    const encoded = transform.request(event.query)
//    let _filter = new mingo.Query(encoded.q.find)
//    if (_filter.test(message)) {
//      //let decoded = bcode.decode(message)
//      let decoded = transform.response(message)
//      try {
//        if (encoded.r && encoded.r.f) {
//          try {
//            let result = await jq.run(encoded.r.f, [decoded])
//            return result
//          } catch (e) {
//            console.log("#############################")
//            console.log("## Error")
//            console.log(e)
//            console.log("## Processor:", encoded.r.f)
//            console.log("## Data:", [decoded])
//            console.log("#############################")
//            return
//          }
//        } else {
//          return [decoded]
//        }
//      } catch (e) {
//        return
//      }
//    } else {
//      return
//    }
//  }
//}
var lookup = function(address, query, key, resfilter, debug) {
  let db = Manager[address].db
  let collection = db.collection(key)
  return new Promise(function(resolve, reject) {
    let cursor
    if (query.find || query.aggregate) {
      if (query.find) {
        cursor = collection.find(query.find, { allowDiskUse:true })
      } else if (query.aggregate) {
        cursor = collection.aggregate(query.aggregate, { allowDiskUse:true })
      }
      if (query.sort) {
        cursor = cursor.sort(query.sort)
      } else if (sort) {
        cursor = cursor.sort(sort)
      }
      if (query.project) {
        cursor = cursor.project(query.project)
      }
      if (query.skip) {
        cursor = cursor.skip(query.skip)
      }

      if (query.limit) {
        if (query.limit > limit) {
          cursor = cursor.limit(limit)
        } else {
          cursor = cursor.limit(query.limit)
        }
      } else if (limit) {
        cursor = cursor.limit(limit)
      } else {
        cursor = cursor.limit(100)
      }
      if (Manager[address].timeout) {
        cursor = cursor.maxTimeMS(Manager[address].timeout)
      }

      if (debug) {
        cursor.explain().then(function(res) {
          console.log(res)
        })
        resolve({
          name: key,
          items: []
        })
      } else {
        cursor.toArray(function(err, docs) {
          console.log("Query = ", query)
          if (err) {
            reject(err)
          } else {
            // "decode": transform the query result into user facing format
            console.log("before transform = ", docs)
            let res = (transform && transform.response) ? transform.response(docs) : docs
            console.log("after transform = ", res)
            if (resfilter && resfilter.f && res.length > 0) {
              let f = resfilter.f;
              console.log("running jq")
              jq.run(f, res).then(function(result) {
                console.log("success jq")
                resolve({
                  name: key,
                  items: result
                })
              }).catch(function(e) {
                console.log("error jq", e)
                reject(e)
              })
            } else {
              console.log("no filter. returning immediately")
              resolve({
                name: key,
                items: res
              })
            }
          }
        })
      }
    } else if (query.count) {
      let count = collection.find(query.count, { allowDiskUse:true }).count(function (e, count) {
        if (e) {
          console.log("Error", e)
          reject(e)
        } else {
          resolve({ name: key, items: count })
        }
      });
    } else if (query.distinct) {
      if (query.distinct.field) {
        collection.distinct(query.distinct.field, query.distinct.query, query.distinct.options).then(function(docs) {
          let res = (transform && transform.response) ? transform.response(docs) : docs
          resolve({
            name: key,
            items: res
          })
        }).catch(function(e) {
          console.log("Error", e)
          reject(e)
        })
      }
    }
  })
}
module.exports = {
  init: init,
  exit: exit,
  read: read,
  validate: validate
}
