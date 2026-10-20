// A CommonJS application consuming the built package. On Nest 12 (ESM-only) this
// goes through require(esm).
require("reflect-metadata")
const runApp = require("./run-app.cjs")

runApp({
  common: require("@nestjs/common"),
  core: require("@nestjs/core"),
  nevo: require("../../dist"),
  label: "CommonJS"
}).catch((err) => {
  console.error(err)
  process.exit(1)
})
