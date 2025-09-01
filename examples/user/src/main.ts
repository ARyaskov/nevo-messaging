import { AppModule } from "./app.module"
import { createKafkaMicroservice } from "@riaskov/nevo-messaging"

createKafkaMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8086
}).then()
