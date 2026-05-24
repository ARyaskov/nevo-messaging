import { NextResponse } from "next/server"
import { getRegistry } from "@/lib/devtools-source"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function GET() {
  return NextResponse.json({ circuits: getRegistry().listCircuits() })
}
