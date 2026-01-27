import { createHttpMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createHttpMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8090,
  host: "0.0.0.0"
}).catch((error) => {
  console.error(error)
})
