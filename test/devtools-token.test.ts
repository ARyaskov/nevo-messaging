import { test, after } from "node:test"
import assert from "node:assert/strict"

// A static import of the .tsx module fails `tsc -p tsconfig.test.json`
// (that config sets no --jsx flag), so the specifier stays computed and is
// only resolved at runtime by node --import tsx.
const mutatePath = "../src/devtools-ui/client/lib/mutate" + ".tsx"

type MutateModule = {
  getDevToolsToken(): string
  setDevToolsToken(token: string): void
}

const loadModule = async (): Promise<MutateModule> => (await import(mutatePath)) as MutateModule

// Module state (memoryToken/memoryWins) lives for the process lifetime, so
// tests run in order (baseline first) and each installs its own storage stub.
const originalDescriptor = Object.getOwnPropertyDescriptor(globalThis, "localStorage")

interface FakeStorage {
  getItem?: (key: string) => string | null
  setItem?: (key: string, value: string) => void
  removeItem?: (key: string) => void
}

function installStorage(storage: FakeStorage | null): void {
  if (storage === null) {
    if (originalDescriptor) Object.defineProperty(globalThis, "localStorage", originalDescriptor)
    else delete (globalThis as { localStorage?: unknown }).localStorage
  } else {
    Object.defineProperty(globalThis, "localStorage", { value: storage, configurable: true, writable: true })
  }
}

after(() => {
  installStorage(null)
})

test("baseline: no write failure — the stored value is still preferred", async () => {
  const { getDevToolsToken } = await loadModule()
  const stored = "stored-value"
  installStorage({ getItem: () => stored })

  assert.equal(getDevToolsToken(), "stored-value")
})

test("set: read works but write fails — the fresh memory token wins over the stale stored value", async () => {
  const { getDevToolsToken, setDevToolsToken } = await loadModule()
  const stored = "stale-stored"
  let writeAttempted = false
  installStorage({
    getItem: () => stored,
    setItem: () => {
      writeAttempted = true
      throw new Error("quota exceeded")
    }
  })

  setDevToolsToken("fresh-token")

  assert.equal(writeAttempted, true, "the test must exercise the write-failure path")
  assert.equal(getDevToolsToken(), "fresh-token")
})

test("clear: read works but remove fails — the cleared memory token wins over the stale stored value", async () => {
  const { getDevToolsToken, setDevToolsToken } = await loadModule()
  let stored = "stale-stored"
  let removeAttempted = false
  installStorage({
    getItem: () => stored,
    setItem: (_key, value) => {
      stored = value
    },
    removeItem: () => {
      removeAttempted = true
      throw new Error("storage denied")
    }
  })

  setDevToolsToken("")

  assert.equal(removeAttempted, true, "the test must exercise the clear-failure path")
  assert.equal(getDevToolsToken(), "")
})

test("a successful write clears the in-memory override so later stored changes are read again", async () => {
  const { getDevToolsToken, setDevToolsToken } = await loadModule()
  let stored = "stored-value"
  let failNextWrite = true
  installStorage({
    getItem: () => stored,
    setItem: (_key, value) => {
      if (failNextWrite) throw new Error("quota exceeded")
      stored = value
    },
    removeItem: () => {
      stored = ""
    }
  })

  // Write fails: memory override is on.
  setDevToolsToken("override")
  assert.equal(getDevToolsToken(), "override")

  // Write succeeds: override must reset so cross-tab/stored updates win again.
  failNextWrite = false
  setDevToolsToken("final")
  stored = "changed-elsewhere"
  assert.equal(getDevToolsToken(), "changed-elsewhere")
})
