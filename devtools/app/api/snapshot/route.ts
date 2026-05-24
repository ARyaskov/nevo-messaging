import { NextResponse } from "next/server"
import { snapshot } from "@/lib/devtools-source"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function GET() {
  return NextResponse.json({ events: snapshot(500) })
}
