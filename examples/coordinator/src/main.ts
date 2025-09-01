import { createKafkaMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createKafkaMicroservice({
  microserviceName: "coordinator",
  module: AppModule,
  port: 8085
}).then()
