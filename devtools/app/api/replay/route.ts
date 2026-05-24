import { NextRequest, NextResponse } from "next/server"
import { getDevToolsBus } from "@riaskov/nevo-messaging"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => null)) as { service?: string; method?: string; params?: unknown; uuid?: string } | null
  if (!body || !body.service || !body.method) {
    return NextResponse.json({ ok: false, error: "missing service/method" }, { status: 400 })
  }

  const bus = getDevToolsBus()
  bus.publish({
    ts: Date.now(),
    type: "custom",
    service: body.service,
    method: body.method,
    extra: { kind: "replay-requested", originalUuid: body.uuid, params: body.params }
  })

  return NextResponse.json({ ok: true, queued: true })
}
