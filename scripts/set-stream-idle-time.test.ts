/**
 * Runs `set-stream-idle-time` against a store started here: the release binary
 * (RUST_SERVER_BIN, else target/release/durable-streams-server), in WAL mode so
 * `/_admin/inventory` is served. Never a deployed store.
 *
 *   cargo build --release && bun run test:scripts
 */
import { execFile, execFileSync, spawn, spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import * as path from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { listStreams } from './set-stream-idle-time.mjs'
import type { ChildProcess } from 'node:child_process'
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
    const proxy = createServer(async (req, res) => {
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(chunk as Buffer)
      if (req.method === `PATCH` && req.url === `/f.2`) {
        attempts += 1
        res.writeHead(503).end(`unavailable`)
        return
      }
      const upstream = await fetch(`${baseUrl}${req.url}`, {
        method: req.method,
        headers: req.headers as Record<string, string>,
        body: chunks.length ? Buffer.concat(chunks) : undefined,
      })
      res.writeHead(upstream.status, Object.fromEntries(upstream.headers))
      res.end(Buffer.from(await upstream.arrayBuffer()))
    })
    await new Promise<void>((resolve) => proxy.listen(0, `127.0.0.1`, resolve))
    const proxyUrl = `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`
    try {
      const result = await run([
        `--base-url`,
        proxyUrl,
        `--prefix`,
        `f.`,
        `--idle`,
        `120`,
        `--commit`,
        `--rate`,
        `50`,
      ])
      expect(result.code).not.toBe(0)
      expect(attempts).toBeGreaterThan(1)
      expect(result.stderr).toContain(`/f.2`)
      expect(result.stdout).toMatch(/\b1 failed\b/)
      expect(await idleOf(`/f.1`)).toBe(`120`)
      expect(await idleOf(`/f.2`)).toBeNull()
      expect(await idleOf(`/f.3`)).toBe(`120`)
    } finally {
      await new Promise((resolve) => proxy.close(resolve))
    }
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

  it(`refuses a bad idle time and a missing prefix without touching the store`, async () => {
    const bad = await run([`--base-url`, baseUrl, `--prefix`, `a.`, `--idle`, `06`])
    expect(bad.code).toBe(2)
    const noPrefix = await run([`--base-url`, baseUrl, `--idle`, `60`, `--commit`])
    expect(noPrefix.code).toBe(2)
  })
})

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
