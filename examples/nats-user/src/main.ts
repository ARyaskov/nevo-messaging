import { createNatsMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createNatsMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8087,
  host: "0.0.0.0"
}).catch((error) => {
  console.error(error)
})
