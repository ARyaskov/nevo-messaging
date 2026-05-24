import { randomFillSync } from "node:crypto"

const POOL_SIZE = 4096
const pool = Buffer.allocUnsafe(POOL_SIZE)
let poolOffset = POOL_SIZE

function refill(): void {
  randomFillSync(pool)
  poolOffset = 0
}

function nextRandomBytes(n: number, target: Uint8Array, offset: number): void {
  if (poolOffset + n > POOL_SIZE) refill()
  target.set(pool.subarray(poolOffset, poolOffset + n), offset)
  poolOffset += n
}

const HEX: string[] = []
for (let i = 0; i < 256; i++) HEX.push((i < 16 ? "0" : "") + i.toString(16))

let lastMs = 0
let monotonicCounter = 0

export function uuidv7(): string {
  let ms = Date.now()
  if (ms === lastMs) {
    monotonicCounter = (monotonicCounter + 1) & 0x0fff
    if (monotonicCounter === 0) ms = lastMs + 1
  } else {
    monotonicCounter = 0
  }
  lastMs = ms

  const buf = new Uint8Array(16)

  buf[0] = (ms / 2 ** 40) & 0xff
  buf[1] = (ms / 2 ** 32) & 0xff
  buf[2] = (ms >>> 24) & 0xff
  buf[3] = (ms >>> 16) & 0xff
  buf[4] = (ms >>> 8) & 0xff
  buf[5] = ms & 0xff

  buf[6] = 0x70 | ((monotonicCounter >>> 8) & 0x0f)
  buf[7] = monotonicCounter & 0xff

  nextRandomBytes(8, buf, 8)
  buf[8] = (buf[8] & 0x3f) | 0x80

  return (
    HEX[buf[0]] + HEX[buf[1]] + HEX[buf[2]] + HEX[buf[3]] + "-" +
    HEX[buf[4]] + HEX[buf[5]] + "-" +
    HEX[buf[6]] + HEX[buf[7]] + "-" +
    HEX[buf[8]] + HEX[buf[9]] + "-" +
    HEX[buf[10]] + HEX[buf[11]] + HEX[buf[12]] + HEX[buf[13]] + HEX[buf[14]] + HEX[buf[15]]
  )
}

export function uuidv7Timestamp(uuid: string): number {
  const ms =
    parseInt(uuid.slice(0, 2), 16) * 2 ** 40 +
    parseInt(uuid.slice(2, 4), 16) * 2 ** 32 +
    parseInt(uuid.slice(4, 6) + uuid.slice(6, 8), 16) * 2 ** 16 +
    parseInt(uuid.slice(9, 13), 16)
  return ms
}
