// Invalidation-only bridge to the existing effect-watch owner. The Rust daemon
// owns identity, eligibility, health probes, atomic writes and restoration.
import path from "node:path"
import { pathToFileURL } from "node:url"

const [file, source] = process.argv.slice(2)
if (!file || !source || !path.isAbsolute(file) || !path.isAbsolute(source)) {
  console.error("service-watch requires an absolute registration file and OpenCode source root")
  process.exit(2)
}

const { FileSystemWatchClient } = await import(
  pathToFileURL(path.join(source, "packages/util/src/filesystem-watch-client.ts")).href
)
const { FileSystemWatch } = await import(
  pathToFileURL(path.join(source, "packages/util/src/filesystem-watch.ts")).href
)
const options = FileSystemWatch.fromEnvironment(process.env)
if (options.enabled === false) {
  process.stdout.write("disabled\n")
  process.exit(0)
}
const client = FileSystemWatchClient.create(options)
let pending: ReturnType<typeof setTimeout> | undefined
const stop = Promise.withResolvers<void>()
const watch = client.watch(
  path.dirname(file),
  (changed: string) => {
    if (path.resolve(changed) !== file || pending !== undefined) return
    pending = setTimeout(() => {
      pending = undefined
      process.stdout.write("change\n")
    }, 75)
  },
  () => {
    // Causes may contain peer-supplied data; only a fixed failure marker leaves.
    process.stdout.write("error\n")
    process.exitCode = 1
    stop.resolve()
  },
)
process.stdout.write("ready\n")
process.on("SIGTERM", () => stop.resolve())
process.on("SIGINT", () => stop.resolve())
await stop.promise
if (pending !== undefined) clearTimeout(pending)
watch.close()
await client.dispose()
