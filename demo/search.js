var bitquery = require('../index')
var bql = {
  "v": 3,
  "q": { "find": { "$text": { "$search": "\"bitcoin cash\"" } }, "db": ["c"] }
};
(async function() {
  let db = await bitquery.init({
    url: process.env.url ? process.env.url : "mongodb://localhost:27017",
    timeout: 3000
  })
  let response = await db.read(bql)
  console.log("Response = ", JSON.stringify(response, null, 2))
  db.exit()
})();
