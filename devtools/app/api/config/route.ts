import { NextRequest, NextResponse } from "next/server"
import { getDevToolsRegistry } from "@riaskov/nevo-messaging"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

interface ConfigUpdate {
  service: string
  accessControl?: { rules?: Array<{ topic?: string; method?: string; allow?: string[]; deny?: string[] }>; allowAllByDefault?: boolean; logDenied?: boolean }
}

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => null)) as ConfigUpdate | null
  if (!body || !body.service) {
    return NextResponse.json({ ok: false, error: "missing service" }, { status: 400 })
  }
  const reg = getDevToolsRegistry()
  const info = reg.getService(body.service)
  if (!info) return NextResponse.json({ ok: false, error: "unknown service" }, { status: 404 })

  if (body.accessControl) {
    reg.registerService({
      ...info,
      accessControl: body.accessControl
    })
  }

  return NextResponse.json({ ok: true, applied: { service: body.service } })
}

export async function GET(req: NextRequest) {
  const service = new URL(req.url).searchParams.get("service")
  const reg = getDevToolsRegistry()
  if (!service) return NextResponse.json({ services: reg.listServices() })
  const info = reg.getService(service)
  if (!info) return NextResponse.json({ ok: false, error: "unknown service" }, { status: 404 })
  return NextResponse.json({ service: info })
}
