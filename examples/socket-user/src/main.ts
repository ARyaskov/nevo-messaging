import { createSocketMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createSocketMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8092,
  host: "0.0.0.0"
}).catch((error) => {
  console.error(error)
})
