import { NextResponse } from "next/server"
import { getRegistry } from "@/lib/devtools-source"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function GET() {
  const reg = getRegistry()
  return NextResponse.json({
    services: reg.listServices(),
    circuits: reg.listCircuits()
  })
}
