// Run after `cargo build`: bun run test/compatibility/clients.ts
// Uses the sibling checkout's real Bus, HTTP routes, and unmodified client APIs.
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { resolve } from "node:path"
import {
	Context,
	Effect,
	Layer,
	Schema,
} from "../../../opencode-bus-smart/packages/server/node_modules/effect/dist/index.js"
import {
	HttpEffect,
	HttpRouter,
	HttpServer,
} from "../../../opencode-bus-smart/packages/server/node_modules/effect/dist/http/index.js"
import { createRoot } from "../../../opencode-bus-smart/packages/client/node_modules/solid-js/dist/server.js"
import { Bus } from "../../../opencode-bus-smart/packages/core/src/bus"
import { SessionEvent } from "../../../opencode-bus-smart/packages/schema/src/session-event"
import { SessionID } from "../../../opencode-bus-smart/packages/schema/src/session-id"
import { SessionMessage } from "../../../opencode-bus-smart/packages/schema/src/session-message"
import { Location } from "../../../opencode-bus-smart/packages/schema/src/location"
import { AbsolutePath } from "../../../opencode-bus-smart/packages/schema/src/schema"
import { createRoutes } from "../../../opencode-bus-smart/packages/server/src/routes"
import { OpenCode } from "../../../opencode-bus-smart/packages/client/src/promise/client"
import { createControlledEventFeed } from "../../../opencode-bus-smart/packages/client/src/solid/controlled-event-feed"

const root = resolve(import.meta.dir, "../..")
const binary =
	process.env.BUS_PROXY_BINARY ??
	resolve(root, "target/debug/opencode-bus-proxy")
const password = crypto.randomUUID()
const headers = {
	authorization: `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`,
}
const location = Location.Ref.make({ directory: AbsolutePath.make(root) })
const firstSession = SessionID.make("ses_proxy_fixture_a")
const secondSession = SessionID.make("ses_proxy_fixture_b")
const assistantMessageID = SessionMessage.ID.make("msg_proxy_fixture")
const marker = Bus.ephemeral({
	type: "rpc.proxy-fixture.barrier",
	schema: { label: Schema.String },
})
const temp = await mkdtemp("/home/rektide/tmp-opencode/bus-proxy-compat-")

try {
	await Effect.runPromise(
		Effect.gen(function* () {
			const context = yield* Layer.build(
				createRoutes({
					app: { version: "proxy-compatibility-test" },
					database: { path: ":memory:" },
					password,
					config: { directory: temp, project: false, content: "{}" },
					fs: { filewatcher: false },
				}).pipe(Layer.provide(HttpServer.layerServices)),
			)
			const bus = Context.get(context, Bus.Service)
			const handler = Context.get(context, HttpRouter.HttpRouter)
				.asHttpEffect()
				.pipe(HttpEffect.toWebHandlerWith(context))
			yield* Effect.promise(async () => {
				const requests = { source: 0, controlled: 0, legacy: 0 }
				const origin = Bun.serve({
					hostname: "127.0.0.1",
					port: 0,
					idleTimeout: 0,
					fetch(request) {
						const path = new URL(request.url).pathname
						if (path === "/api/experimental/event/source") requests.source++
						if (path === "/api/experimental/event") requests.controlled++
						if (path === "/api/event") requests.legacy++
						return handler(request)
					},
				})
				const child = Bun.spawn(
					[
						binary,
						"--upstream",
						origin.url.toString(),
						"--listen",
						"127.0.0.1:0",
						"--log-json",
					],
					{
						env: { ...process.env, OPENCODE_SERVER_PASSWORD: password },
						stdout: "pipe",
						stderr: "inherit",
					},
				)
				const controllers: AbortController[] = []
				const disposers: (() => void)[] = []
				const logs: string[] = []
				const address = Promise.withResolvers<string>()
				const logging = (async () => {
					let pending = ""
					for await (const chunk of child.stdout) {
						pending += Buffer.from(chunk).toString()
						const lines = pending.split("\n")
						pending = lines.pop() ?? ""
						for (const line of lines) {
							logs.push(line)
							const entry = JSON.parse(line)
							if (entry.fields?.message === "event gateway listening")
								address.resolve(`http://${entry.fields.address}`)
						}
					}
					address.reject(
						new Error(`Proxy exited before listening: ${logs.join("\n")}`),
					)
				})()
				try {
					const baseUrl = await deadline(address.promise)
					await ready(baseUrl)
					assert.equal((await fetch(`${baseUrl}/api/event`)).status, 401)
					const api = OpenCode.make({ baseUrl, headers })
					const direct = OpenCode.make({
						baseUrl: origin.url.toString(),
						headers,
					})
					const signal = () => {
						const controller = new AbortController()
						controllers.push(controller)
						return controller.signal
					}
					const legacy = api.event
						.subscribe({ signal: signal() })
						[Symbol.asyncIterator]()
					assert.equal(
						(await deadline(legacy.next())).value?.type,
						"server.connected",
					)
					const focused = async (
						client: ReturnType<typeof OpenCode.make>,
						sessionID: string,
					) => {
						const feed = createRoot((dispose) => {
							disposers.push(dispose)
							return createControlledEventFeed({ removalGrace: 0 })
						})
						feed.setDesired({ sessions: [sessionID] })
						const iterator = feed
							.subscribe(client, signal(), { onActivity() {} })
							[Symbol.asyncIterator]()
						assert.equal(
							(await deadline(iterator.next())).value?.type,
							"server.connected",
						)
						await deadline(feed.flush())
						assert.equal(feed.mode(), "controlled")
						return { feed, iterator }
					}
					const first = await focused(api, firstSession)
					const second = await focused(
						OpenCode.make({ baseUrl, headers }),
						secondSession,
					)
					const directFirst = await focused(direct, firstSession)
					const readers = [
						legacy,
						first.iterator,
						second.iterator,
						directFirst.iterator,
					]
					const publish = async (sessionID: SessionID) => {
						const options = { location }
						const result = [
							await Effect.runPromise(
								bus.publish(
									SessionEvent.Text.Delta,
									{ sessionID, assistantMessageID, ordinal: 0, delta: "hello" },
									options,
								),
							),
							await Effect.runPromise(
								bus.publish(
									SessionEvent.Reasoning.Delta,
									{
										sessionID,
										assistantMessageID,
										ordinal: 0,
										delta: "reason",
									},
									options,
								),
							),
							await Effect.runPromise(
								bus.publish(
									SessionEvent.Tool.Input.Delta,
									{
										sessionID,
										assistantMessageID,
										id: "fixture-tool",
										delta: "{}",
									},
									options,
								),
							),
							await Effect.runPromise(
								bus.publish(
									SessionEvent.Tool.Progress,
									{
										sessionID,
										assistantMessageID,
										id: "fixture-tool",
										metadata: { progress: 1 },
									},
									options,
								),
							),
							await Effect.runPromise(
								bus.publish(
									SessionEvent.Compaction.Delta,
									{ sessionID, text: "compact" },
									options,
								),
							),
						]
						return result.map((event) => event.id)
					}
					const batches = readers.map((reader) => throughBarrier(reader))
					const firstIDs = await publish(firstSession)
					const secondIDs = await publish(secondSession)
					const barrier = await Effect.runPromise(
						bus.publish(marker, { label: "initial" }, { location }),
					)
					const received = await deadline(Promise.all(batches))
					assert.deepEqual(received, [
						[...firstIDs, ...secondIDs, barrier.id],
						[...firstIDs, barrier.id],
						[...secondIDs, barrier.id],
						[...firstIDs, barrier.id],
					])

					first.feed.setDesired({ sessions: [secondSession] })
					// Even a zero removal grace expires in a timer; flush first sees the held union.
					await new Promise((resolve) => setTimeout(resolve, 0))
					await deadline(first.feed.flush())
					assert.deepEqual(first.feed.installed()?.sessions, [secondSession])
					const changed = readers.map((reader) => throughBarrier(reader))
					const firstAfter = await publish(firstSession)
					const secondAfter = await publish(secondSession)
					const changedBarrier = await Effect.runPromise(
						bus.publish(marker, { label: "replaced" }, { location }),
					)
					assert.deepEqual(await deadline(Promise.all(changed)), [
						[...firstAfter, ...secondAfter, changedBarrier.id],
						[...secondAfter, changedBarrier.id],
						[...secondAfter, changedBarrier.id],
						[...firstAfter, changedBarrier.id],
					])

					assert.deepEqual(requests, { source: 1, controlled: 1, legacy: 0 })
					const inspection = await fetch(`${baseUrl}/_bus/subscriptions`, {
						headers,
					})
					assert.equal(inspection.status, 200)
					assert.match(await inspection.text(), /ses_proxy_fixture_b/)
					assert.equal(
						(await api.server.info()).version,
						"proxy-compatibility-test",
					)
					console.log(
						"PASS: real Bus -> one origin source -> Rust proxy -> unchanged legacy and bus-smart clients; all five types, broad RPC, live interest replacement, direct-origin smart client, auth and HTTP forwarding",
					)
				} finally {
					controllers.forEach((controller) => controller.abort())
					disposers.forEach((dispose) => dispose())
					child.kill("SIGTERM")
					const exit = await deadline(child.exited).catch(() => {
						child.kill("SIGKILL")
						return child.exited
					})
					await logging
					await origin.stop(true)
					assert.equal(exit, 0, logs.join("\n"))
				}
			})
		}).pipe(Effect.scoped),
	)
} finally {
	await rm(temp, { recursive: true, force: true })
}

async function throughBarrier(
	reader: AsyncIterator<{ id?: string; type: string }>,
) {
	const ids: string[] = []
	while (true) {
		const event = await reader.next()
		assert.equal(event.done, false)
		if (event.value.id) ids.push(event.value.id)
		if (event.value.type === marker.type) return ids
	}
}

async function ready(baseUrl: string) {
	for (let attempt = 0; attempt < 100; attempt++) {
		const response = await fetch(`${baseUrl}/_bus/health`, { headers })
		await response.body?.cancel()
		if (response.ok) return
		await Bun.sleep(100)
	}
	throw new Error("Proxy source did not become ready")
}

async function deadline<T>(promise: Promise<T>): Promise<T> {
	let timer: ReturnType<typeof setTimeout>
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Compatibility check timed out")),
					15000,
				)
			}),
		])
	} finally {
		clearTimeout(timer!)
	}
}
