/**
 * Runs `set-stream-idle-time` against a store started here: the release binary
 * (RUST_SERVER_BIN, else target/release/durable-streams-server), in WAL mode so
 * `/_admin/inventory` is served. Never a deployed store.
 *
 *   cargo build --release && bun run test:scripts
 */
import { execFile, execFileSync, spawn, spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer, request as httpRequest } from 'node:http'
import { tmpdir } from 'node:os'
import * as path from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { listStreams } from './set-stream-idle-time.mjs'
import type { ChildProcess } from 'node:child_process'
import type { IncomingMessage } from 'node:http'
import type { AddressInfo } from 'node:net'

const script = path.join(__dirname, `set-stream-idle-time.mjs`)
const storeId = `7d0f3c1e-5b8a-4e7f-9c2d-1a6b3e4f5d60`
const storeGeneration = `3a9e2b7c-1d4f-4c8e-8b6a-5f0d2e9c7b14`
const filesystemUuid = `c5e8a1f4-2b7d-4e3a-9f6c-8d1b0a7e5c32`
const artifactDigest = `sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef`

let server: ChildProcess | null = null
let dataDir = ``
let baseUrl = ``

beforeAll(async () => {
  const binary = resolveServerBinary()
  dataDir = mkdtempSync(path.join(tmpdir(), `ds-idle-script-`))
  const layout = [
    `--store-id`,
    storeId,
    `--store-generation`,
    storeGeneration,
    `--protocol-version`,
    `1`,
    `--layout-version`,
    `1`,
    `--wal-shards`,
    `1`,
    `--stream-lanes`,
    `1`,
    `--filesystem-uuid`,
    filesystemUuid,
  ]
  const bootstrap = spawnSync(
    binary,
    [
      `bootstrap-store`,
      `--data-dir`,
      dataDir,
      `--durability-mode`,
      `wal`,
      `--creation-time`,
      `2026-11-25T00:00:00Z`,
      ...layout,
    ],
    { encoding: `utf8` }
  )
  if (bootstrap.status !== 0) {
    throw new Error(`Could not bootstrap the test store: ${bootstrap.stderr}`)
  }
  const port = await freePort()
  baseUrl = `http://127.0.0.1:${port}`
  server = spawn(
    binary,
    [
      `--port`,
      String(port),
      `--data-dir`,
      dataDir,
      `--artifact-digest`,
      artifactDigest,
      ...layout,
    ],
    { stdio: [`ignore`, `ignore`, `pipe`] }
  )
  server.stderr?.on(`data`, (d: Buffer) => process.stderr.write(`[store] ${d}`))
  await waitForServer(baseUrl, 15_000)
})

afterAll(async () => {
  server?.kill(`SIGTERM`)
  await new Promise((resolve) => setTimeout(resolve, 300))
  if (dataDir) rmSync(dataDir, { recursive: true, force: true })
})

describe(`set-stream-idle-time`, () => {
  it(`dry run prints the count and bytes and changes nothing; --commit sets only the prefix`, async () => {
    const sizes = { '/a.1': 10, '/a.2': 0, '/a.3': 2500 }
    for (const [stream, size] of Object.entries(sizes)) {
      await createStream(stream, size)
    }
    await createStream(`/b.1`, 7)
    await createStream(`/b.2`, 0)
    const all = [...Object.keys(sizes), `/b.1`, `/b.2`]

    const dry = await run([`--base-url`, baseUrl, `--prefix`, `a.`, `--idle`, `600`])
    expect(dry.code, dry.stderr).toBe(0)
    expect(dry.stdout).toMatch(/\b3 streams\b/)
    expect(dry.stdout).toMatch(/\b2510 bytes\b/)
    expect(dry.stdout).toMatch(/dry run/i)
    for (const stream of all) {
      expect(await idleOf(stream), stream).toBeNull()
    }

    const commit = await run([
      `--base-url`,
      baseUrl,
      `--prefix`,
      `a.`,
      `--idle`,
      `600`,
      `--commit`,
      `--rate`,
      `50`,
    ])
    expect(commit.code, commit.stderr).toBe(0)
    expect(commit.stdout).toMatch(/\b3 changed\b/)
    for (const stream of Object.keys(sizes)) {
      expect(await idleOf(stream), stream).toBe(`600`)
    }
    expect(await idleOf(`/b.1`)).toBeNull()
    expect(await idleOf(`/b.2`)).toBeNull()

    // Running it again changes nothing and still succeeds.
    const again = await run([
      `--base-url`,
      baseUrl,
      `--prefix`,
      `a.`,
      `--idle`,
      `600`,
      `--commit`,
      `--rate`,
      `50`,
    ])
    expect(again.code, again.stderr).toBe(0)
    for (const stream of Object.keys(sizes)) {
      expect(await idleOf(stream), stream).toBe(`600`)
    }

    // `never` and `default` go through as the store spells them.
    const never = await run([
      `--base-url`,
      `${baseUrl}/`,
      `--prefix`,
      `/a.`,
      `--idle`,
      `never`,
      `--commit`,
    ])
    expect(never.code, never.stderr).toBe(0)
    expect(await idleOf(`/a.2`)).toBe(`never`)
    const reset = await run([
      `--base-url`,
      baseUrl,
      `--prefix`,
      `a.`,
      `--idle`,
      `default`,
      `--commit`,
    ])
    expect(reset.code, reset.stderr).toBe(0)
    for (const stream of all) {
      expect(await idleOf(stream), stream).toBeNull()
    }
  })

  it(`retries a failing stream, reports it and exits non-zero`, async () => {
    for (const stream of [`/f.1`, `/f.2`, `/f.3`]) await createStream(stream, 1)
    let attempts = 0
    const result = await withProxy(
      async (req) => {
        if (req.method === `PATCH` && req.url === `/f.2`) {
          attempts += 1
          return { status: 503, body: `unavailable` }
        }
      },
      (proxyUrl) => commit(proxyUrl, `f.`, `120`)
    )
    expect(result.code).not.toBe(0)
    expect(attempts).toBeGreaterThan(1)
    expect(result.stderr).toContain(`/f.2`)
    expect(result.stdout).toMatch(/\b1 failed\b/)
    expect(await idleOf(`/f.1`)).toBe(`120`)
    expect(await idleOf(`/f.2`)).toBeNull()
    expect(await idleOf(`/f.3`)).toBe(`120`)
  })

  it(`paces every attempt, retries included`, async () => {
    await createStream(`/r.1`, 1)
    const starts: number[] = []
    const result = await withProxy(
      async (req) => {
        if (req.method !== `PATCH`) return
        starts.push(Date.now())
        if (starts.length < 3) return { status: 503, body: `unavailable` }
      },
      (proxyUrl) => commit(proxyUrl, `r.`, `90`, `1`)
    )
    expect(result.code, result.stderr).toBe(0)
    expect(starts).toHaveLength(3)
    // Backoff alone would start the retries 0.5 s and 1 s apart.
    for (let i = 1; i < starts.length; i++) {
      expect(starts[i] - starts[i - 1], `gap before attempt ${i + 1}`).toBeGreaterThanOrEqual(950)
    }
    expect(await idleOf(`/r.1`)).toBe(`90`)
  })

  it(`skips a stream only when HEAD confirms it is gone`, async () => {
    for (const stream of [`/d.1`, `/d.2`, `/d.3`, `/d.4`]) await createStream(stream, 1)
    // A fork keeps /d.3 as a tombstone once deleted, which answers 410; /d.2
    // is removed outright and answers 404.
    const fork = await fetch(`${baseUrl}/dfork.3`, {
      method: `PUT`,
      headers: {
        'content-type': `application/octet-stream`,
        'stream-forked-from': `/d.3`,
        'stream-fork-offset': `now`,
      },
    })
    expect(fork.status).toBe(201)
    const seen: Record<string, number[]> = {}
    const result = await withProxy(
      async (req) => {
        if (req.method === `PATCH` && (req.url === `/d.2` || req.url === `/d.3`)) {
          const deleted = await fetch(`${baseUrl}${req.url}`, { method: `DELETE` })
          expect(deleted.status, `delete ${req.url}`).toBe(204)
        }
      },
      (proxyUrl) => commit(proxyUrl, `d.`, `300`),
      (req, status) => {
        if (req.method !== `GET`) (seen[`${req.method} ${req.url}`] ??= []).push(status)
      }
    )
    expect(seen).toEqual({
      'PATCH /d.1': [204],
      'PATCH /d.2': [404],
      'HEAD /d.2': [404],
      'PATCH /d.3': [410],
      'HEAD /d.3': [410],
      'PATCH /d.4': [204],
    })
    expect(result.code, result.stderr).toBe(0)
    expect(result.stdout).toContain(`skipped (deleted since listing): /d.2`)
    expect(result.stdout).toContain(`skipped (deleted since listing): /d.3`)
    expect(result.stdout).toMatch(/\b2 changed, 2 skipped \(deleted since listing\), 0 failed\b/)
    expect(await idleOf(`/d.1`)).toBe(`300`)
    expect(await idleOf(`/d.4`)).toBe(`300`)
  })

  it(`fails a 404 to PATCH when HEAD finds the stream`, async () => {
    for (const stream of [`/g.1`, `/g.2`]) await createStream(stream, 1)
    const result = await withProxy(
      async (req) => {
        if (req.method === `PATCH` && req.url === `/g.2`) {
          return { status: 404, body: `no route` }
        }
      },
      (proxyUrl) => commit(proxyUrl, `g.`, `45`)
    )
    expect(result.code).toBe(1)
    expect(result.stderr).toContain(`failed: /g.2: PATCH answered 404 no route but HEAD answered 200`)
    expect(result.stdout).toMatch(/\b1 changed, 0 skipped \(deleted since listing\), 1 failed\b/)
    expect(await idleOf(`/g.1`)).toBe(`45`)
    expect(await idleOf(`/g.2`)).toBeNull()
  })

  it(`fails a redirect instead of following it`, async () => {
    for (const stream of [`/h.1`, `/h.2`]) await createStream(stream, 1)
    const result = await withProxy(
      async (req) => {
        if (req.method === `PATCH` && req.url === `/h.2`) {
          // Followed, the PATCH would land on the store and succeed.
          return { status: 307, headers: { location: `${baseUrl}/h.2` } }
        }
      },
      (proxyUrl) => commit(proxyUrl, `h.`, `45`)
    )
    expect(result.code).toBe(1)
    expect(result.stderr).toContain(`failed: /h.2: 307`)
    expect(await idleOf(`/h.1`)).toBe(`45`)
    expect(await idleOf(`/h.2`)).toBeNull()
  })

  it(`sends nothing for a path that URL parsing would rewrite`, async () => {
    // The `..` paths resolve outside the prefix, onto streams of their own.
    const rewritten = [`/dot.a/../decoy.1`, `/dot.b/%2E%2E/decoy.2`, `/dot.c/./e`, `/dot.d\\e`]
    for (const stream of [`/decoy.1`, `/decoy.2`, `/dot.ok`]) await createStream(stream, 1)
    for (const stream of rewritten) expect(await putRaw(stream), stream).toBe(201)
    const sent: string[] = []
    const result = await withProxy(
      async (req) => {
        if (req.method === `PATCH`) sent.push(req.url ?? ``)
      },
      (proxyUrl) => commit(proxyUrl, `dot.`, `30`)
    )
    expect(result.code).toBe(1)
    expect(sent).toEqual([`/dot.ok`])
    for (const stream of rewritten) {
      expect(result.stderr).toContain(`failed: ${stream}: URL parsing would rewrite this path; no PATCH sent`)
    }
    expect(result.stdout).toMatch(/\b1 changed, 0 skipped \(deleted since listing\), 4 failed\b/)
    expect(await idleOf(`/dot.ok`)).toBe(`30`)
    expect(await idleOf(`/decoy.1`)).toBeNull()
    expect(await idleOf(`/decoy.2`)).toBeNull()
  })

  it(`pages on past a run of soft-deleted streams longer than a page's scan`, async () => {
    // Each parent keeps a fork, so deleting it leaves a tombstone. With one
    // stream a page, a page examines 10 entries; 25 tombstones run past that.
    for (let i = 0; i < 25; i++) {
      const parent = `/t.${String(i).padStart(3, `0`)}`
      await createStream(parent, 1)
      const fork = await fetch(`${baseUrl}/tfork.${i}`, {
        method: `PUT`,
        headers: {
          'content-type': `application/octet-stream`,
          'stream-forked-from': parent,
          'stream-fork-offset': `now`,
        },
      })
      expect(fork.status, `fork of ${parent}`).toBe(201)
      const deleted = await fetch(`${baseUrl}${parent}`, { method: `DELETE` })
      expect(deleted.status, `delete ${parent}`).toBe(204)
    }
    await createStream(`/t.zz`, 4)

    const first = await fetch(
      `${baseUrl}/_admin/inventory?prefix=%2Ft.&stable=false&limit=1`
    )
    const page = (await first.json()) as { streams: unknown[]; next_cursor?: string }
    expect(page.streams, `the first page holds only tombstones`).toEqual([])
    expect(page.next_cursor).toBeTruthy()

    const streams = await listStreams({ baseUrl, prefix: `/t.`, pageSize: 1 })
    expect(streams).toEqual([{ path: `/t.zz`, durableBytes: 4 }])

    const dry = await run([`--base-url`, baseUrl, `--prefix`, `t.`, `--idle`, `60`])
    expect(dry.code, dry.stderr).toBe(0)
    expect(dry.stdout).toMatch(/\b1 stream\b/)
    expect(dry.stdout).toMatch(/\b4 bytes\b/)
  })

  it(`refuses bad arguments without touching the store`, async () => {
    const bad = await run([`--base-url`, baseUrl, `--prefix`, `a.`, `--idle`, `06`])
    expect(bad.code).toBe(2)
    const noPrefix = await run([`--base-url`, baseUrl, `--idle`, `60`, `--commit`])
    expect(noPrefix.code).toBe(2)
    for (const rate of [`0`, `0.001`, `1001`, `-1`, `fast`]) {
      const result = await run([`--base-url`, baseUrl, `--prefix`, `a.`, `--idle`, `60`, `--rate`, rate])
      expect(result.code, `--rate ${rate}`).toBe(2)
    }
    const slowest = await run([`--base-url`, baseUrl, `--prefix`, `a.`, `--idle`, `60`, `--rate`, `0.01`])
    expect(slowest.code, slowest.stderr).toBe(0)
  })
})

function commit(url: string, prefix: string, idle: string, rate = `50`) {
  return run([`--base-url`, url, `--prefix`, prefix, `--idle`, idle, `--commit`, `--rate`, rate])
}

type Canned = { status: number; body?: string; headers?: Record<string, string> }

// Runs `fn` against a proxy to the store. `intercept` may answer a request
// itself; otherwise it is forwarded and `observe` sees the store's status.
async function withProxy<T>(
  intercept: (req: IncomingMessage) => Promise<Canned | void>,
  fn: (proxyUrl: string) => Promise<T>,
  observe?: (req: IncomingMessage, status: number) => void
): Promise<T> {
  const proxy = createServer(async (req, res) => {
    const chunks: Buffer[] = []
    for await (const chunk of req) chunks.push(chunk as Buffer)
    const canned = await intercept(req)
    if (canned) {
      res.writeHead(canned.status, canned.headers ?? {}).end(canned.body ?? ``)
      return
    }
    const upstream = await fetch(`${baseUrl}${req.url}`, {
      method: req.method,
      headers: req.headers as Record<string, string>,
      body: chunks.length ? Buffer.concat(chunks) : undefined,
      redirect: `manual`,
    })
    observe?.(req, upstream.status)
    res.writeHead(upstream.status, Object.fromEntries(upstream.headers))
    res.end(Buffer.from(await upstream.arrayBuffer()))
  })
  await new Promise<void>((resolve) => proxy.listen(0, `127.0.0.1`, resolve))
  try {
    return await fn(`http://127.0.0.1:${(proxy.address() as AddressInfo).port}`)
  } finally {
    await new Promise((resolve) => proxy.close(resolve))
  }
}

// Creates a stream at a path exactly as given; fetch would normalize it first.
function putRaw(stream: string): Promise<number> {
  const { hostname, port } = new URL(baseUrl)
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      { host: hostname, port, path: stream, method: `PUT`, headers: { 'content-type': `application/octet-stream` } },
      (res) => {
        res.resume()
        resolve(res.statusCode ?? 0)
      }
    )
    req.on(`error`, reject)
    req.end()
  })
}

async function createStream(stream: string, bytes: number): Promise<void> {
  const created = await fetch(`${baseUrl}${stream}`, {
    method: `PUT`,
    headers: { 'content-type': `application/octet-stream` },
  })
  expect(created.status, `create ${stream}`).toBe(201)
  if (bytes > 0) {
    const appended = await fetch(`${baseUrl}${stream}`, {
      method: `POST`,
      headers: { 'content-type': `application/octet-stream` },
      body: Buffer.alloc(bytes, 0x61),
    })
    expect(appended.status, `append ${stream}`).toBeLessThan(300)
  }
}

async function idleOf(stream: string): Promise<string | null> {
  const head = await fetch(`${baseUrl}${stream}`, { method: `HEAD` })
  expect(head.status, `HEAD ${stream}`).toBe(200)
  return head.headers.get(`stream-tier-after-idle`)
}

function run(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    execFile(process.execPath, [script, ...args], (error, stdout, stderr) => {
      const code = error ? (typeof error.code === `number` ? error.code : 1) : 0
      resolve({ code, stdout, stderr })
    })
  })
}

function resolveServerBinary(): string {
  if (process.env.RUST_SERVER_BIN) return process.env.RUST_SERVER_BIN
  const metadata = JSON.parse(
    execFileSync(`cargo`, [`metadata`, `--format-version=1`, `--no-deps`], {
      cwd: path.join(__dirname, `..`),
      encoding: `utf8`,
    })
  ) as { target_directory: string }
  return path.join(metadata.target_directory, `release`, `durable-streams-server`)
}

async function freePort(): Promise<number> {
  const probe = createServer()
  await new Promise<void>((resolve) => probe.listen(0, `127.0.0.1`, resolve))
  const { port } = probe.address() as AddressInfo
  await new Promise((resolve) => probe.close(resolve))
  return port
}

async function waitForServer(url: string, timeoutMs: number): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    try {
      await fetch(`${url}/health`)
      return
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }
  throw new Error(`The test store did not become ready within ${timeoutMs}ms`)
}
