// Isolated native Promise/Effect discovery + real daemon + reused Effect watcher.
// No managed service, live state, database, model, or installed OpenCode is used.
import assert from "node:assert/strict"
import { mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises"
import path from "node:path"
import { ConfigProvider, Effect, Layer } from "../../../opencode-server-reg/packages/cli/node_modules/effect/dist/index.js"
import { NetAddress } from "../../../opencode-server-reg/packages/cli/node_modules/effect/dist/net/index.js"
import { NodeFileSystem } from "../../../opencode-server-reg/packages/cli/node_modules/@effect/platform-node/dist/index.js"
import { Service } from "../../../opencode-server-reg/packages/client/src/effect/service"
import { discover, ensure } from "../../../opencode-server-reg/packages/client/src/promise/service"
import { ServiceRegistration } from "../../../opencode-server-reg/packages/cli/src/services/service-registration"
import { OPENCODE_VERSION } from "../../../opencode-server-reg/packages/cli/src/version"

const root = path.resolve(import.meta.dir, "../..")
const temporary = await mkdtemp("/home/rektide/tmp-opencode/bus-proxy-discovery-")
const file = path.join(temporary, "service-local.json")
const password = "test-only-discovery-secret"
const authorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`
let identity = { pid: process.pid, version: OPENCODE_VERSION }
let epoch = 0
const origin = Bun.serve({
  hostname: "127.0.0.1", port: 0, idleTimeout: 0,
  fetch(request) {
    if (request.headers.get("authorization") !== authorization) return new Response(null, { status: 401 })
    if (new URL(request.url).pathname === "/api/event") {
      let heartbeat: ReturnType<typeof setInterval>
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(`data: {"type":"server.connected","id":"evt_discovery_${++epoch}","data":{}}\n\n`))
          heartbeat = setInterval(() => controller.enqueue(new TextEncoder().encode(": alive\n\n")), 1000)
        },
        cancel() { clearInterval(heartbeat) },
      }), { headers: { "content-type": "text/event-stream", "OpenCode-Event-Scope": "1" } })
    }
    return Response.json(identity)
  },
})

async function publish(proxy?: string, override?: string) {
  await Effect.runPromise(ServiceRegistration.register({
    mode: "server", address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", origin.port),
    id: crypto.randomUUID(), password, file, shutdown: Effect.void, publishURL: override,
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeFileSystem.layer, ConfigProvider.layer(ConfigProvider.fromUnknown(proxy ? { OPENCODE_PROXY_URL: proxy } : {}))))))
  return JSON.parse(await readFile(file, "utf8"))
}

async function waitFor(check: () => Promise<boolean>, message: string, timeout = 15000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await check()) return
    await Bun.sleep(25)
  }
  throw new Error(message)
}

async function launch(mode: "cooperate" | "stomp", listen = "127.0.0.1:0", disabled = false, source = path.resolve(root, "../opencode-working")) {
  const env = {
    ...process.env, OPENCODE_SERVER_PASSWORD: password,
    OPENCODE_WATCHER_BACKEND: "native", OPENCODE_FILEWATCHER_DISABLE: disabled ? "1" : "0", OPENCODE_DISABLE_FILEWATCHER: "0", RUST_LOG: "info",
  }
  delete env.BUS_PROXY_PASSWORD_FILE
  delete env.BUS_PROXY_CREDENTIAL
  const child = Bun.spawn([
    process.env.BUS_PROXY_BINARY ?? path.join(root, "target/debug/opencode-bus-proxy"),
    "--upstream", origin.url.origin, "--listen", listen, "--service-file", file,
    "--service-mode", mode, "--service-watch-helper", path.join(root, "script/service-watch.ts"),
    "--service-watch-source", source, "--log-json",
  ], { env, stdout: "pipe", stderr: "pipe" })
  const address = Promise.withResolvers<string>()
  const lines: string[] = []
  const logging = (async () => {
    let pending = ""
    for await (const chunk of child.stdout) {
      pending += Buffer.from(chunk).toString()
      const complete = pending.split("\n")
      pending = complete.pop() ?? ""
      for (const line of complete) {
        lines.push(line)
        const entry = JSON.parse(line)
        if (entry.fields?.message === "event gateway listening") address.resolve(`http://${entry.fields.address}`)
      }
    }
    address.reject(new Error(`daemon exited before serving (code ${child.exitCode})`))
  })()
  const stderr = new Response(child.stderr).text()
  const url = await address.promise
  return { child, url, lines, async stop() {
    child.kill("SIGTERM")
    assert.equal(await child.exited, 0, await stderr)
    await logging
  } }
}

async function claimed() {
  return JSON.parse(await readFile(file, "utf8"))._busProxy !== undefined
}

async function nativeClients(url: string) {
  const promise = await discover({ file, version: identity.version })
  const effect = await Effect.runPromise(Service.discover({ file, version: identity.version }).pipe(Effect.provide(NodeFileSystem.layer)))
  assert.equal(promise?.url, url)
  assert.equal(effect?.url, url)
  assert.equal((await ensure({ file, version: identity.version, command: [], onStart() { throw new Error("must not spawn") } })).url, url)
}

let active: Awaited<ReturnType<typeof launch>> | undefined
try {
  active = await launch("cooperate")
  const cooperative = active
  await publish()
  await waitFor(claimed, "cooperative enrichment")
  await nativeClients(origin.url.origin)
  const nativeProxy = await publish(cooperative.url)
  await waitFor(claimed, "native proxy contact enrichment")
  await nativeClients(cooperative.url)
  const contributed = JSON.parse(await readFile(file, "utf8"))
  assert.equal(contributed.pid, identity.pid)
  assert.equal(contributed._busProxy.pid, cooperative.child.pid)
  assert.equal(contributed.discovery.endpoints.proxy.pid, cooperative.child.pid)
  assert.equal(JSON.stringify(contributed).split(password).length, 2)
  assert.equal((await stat(file)).mode & 0o777, 0o600)
  await writeFile(file, JSON.stringify({ ...contributed, pid: cooperative.child.pid }))
  assert.equal(await discover({ file }), undefined, "proxy PID must fail the unchanged origin info probe")
  await writeFile(file, JSON.stringify(contributed))
  await cooperative.stop()
  active = undefined
  assert.deepEqual(JSON.parse(await readFile(file, "utf8")), nativeProxy)

  active = await launch("stomp", new URL(cooperative.url).host)
  const stomp = active
  for (let index = 0; index < 3; index++) {
    const published = await publish(undefined, index === 1 ? "http://localhost:32123" : undefined)
    await waitFor(async () => (await claimed()) && JSON.parse(await readFile(file, "utf8")).id === published.id, "stomp native atomic publication")
    await nativeClients(stomp.url)
  }
  await waitFor(async () => stomp.lines.some((line) => line.includes("Effect discovery watcher ready")), "reused watcher readiness")
  const before = (await stat(file)).mtimeMs
  await Bun.sleep(1300)
  assert.equal((await stat(file)).mtimeMs, before, "own writes must not churn")
  identity = { pid: process.pid + 1000000, version: "fresh-origin-test" }
  const restarted = { mode: "server", id: "fresh-origin", ...identity, password, url: origin.url.origin, future: { preserved: true } }
  await writeFile(file, "{partial")
  await Bun.sleep(200)
  assert.equal(await readFile(file, "utf8"), "{partial", "partial writes remain untouched")
  await writeFile(file, JSON.stringify(restarted))
  await waitFor(claimed, "stomp restarted origin identity")
  await nativeClients(stomp.url)
  assert.deepEqual(JSON.parse(await readFile(file, "utf8")).future, restarted.future)
  await unlink(file)
  await Bun.sleep(150)
  assert.equal(await Bun.file(file).exists(), false, "deletion must not invent a record")
  await writeFile(file, JSON.stringify(restarted))
  await waitFor(claimed, "stomp recreation")
  await stomp.stop()
  active = undefined
  assert.deepEqual(JSON.parse(await readFile(file, "utf8")), restarted)
  active = await launch("stomp", new URL(stomp.url).host, true)
  await waitFor(claimed, "disabled watcher retains polling reconciliation")
  await waitFor(async () => active!.lines.some((line) => line.includes("Effect discovery watcher disabled")), "watcher disable preference")
  await active.stop()
  active = undefined
  active = await launch("stomp", new URL(stomp.url).host, false, path.join(temporary, "missing-source"))
  await waitFor(claimed, "failed watcher retains polling reconciliation")
  await waitFor(async () => active!.lines.some((line) => line.includes("Effect discovery watcher unavailable")), "watcher failure report")
  await active.stop()
  active = undefined
  assert.deepEqual(JSON.parse(await readFile(file, "utf8")), restarted)
  console.log("ok: unchanged Promise/Effect native discovery, cooperative publisher, stomp, reused Effect watcher, restoration")
} finally {
  if (active) { active.child.kill("SIGTERM"); await active.child.exited }
  await origin.stop(true)
  await rm(temporary, { recursive: true, force: true })
}
