#!/usr/bin/env node
// Set, change or remove the idle time (`Stream-Tier-After-Idle`) of the streams
// that already exist under a path prefix, through the store's HTTP interface
// only: `GET /_admin/inventory` to list them, `PATCH` on each to change it.
//
//   node scripts/set-stream-idle-time.mjs --base-url <store> --prefix <path prefix> \
//     --idle <seconds | never | default> [--commit] [--rate <requests per second>]
//
// Without `--commit` it is a dry run: it prints how many streams match and their
// durable bytes, and sends no PATCH. With `--commit` it PATCHes each stream at
// `--rate` a second, retries a request that failed in transit or answered 5xx or
// 429, prints progress, and exits 1 if any stream failed. Running it again is
// safe: repeating the PATCH changes nothing. Stream paths start with `/`; a
// prefix without one gets it.
import { realpathSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'

const USAGE = `usage: set-stream-idle-time --base-url <store> --prefix <path prefix> --idle <seconds | never | default> [--commit] [--rate <requests per second>]`
const DEFAULT_RATE = 10
const PAGE_SIZE = 1000
const MAX_ATTEMPTS = 4
const REQUEST_TIMEOUT_MS = 30_000
const PROGRESS_EVERY_MS = 5_000

// Lists the live streams under `prefix` with `stable=false`. Such a page can hold
// fewer streams than asked for, or none, and still carry a cursor, so this pages
// on until a page has no cursor.
export async function listStreams({ baseUrl, prefix, pageSize = PAGE_SIZE }) {
  const streams = []
  let cursor = null
  do {
    // The cursor is already in the one spelling the store accepts.
    let url = `${baseUrl}/_admin/inventory?prefix=${encodeURIComponent(prefix)}&stable=false&limit=${pageSize}`
    if (cursor) url += `&cursor=${cursor}`
    const { response, error } = await request(url, { method: `GET` })
    if (!response || response.status !== 200) {
      throw new Error(`inventory listing failed: ${describe(response, error)}`)
    }
    const page = await response.json()
    for (const item of page.streams) {
      if (!item.deleted) streams.push({ path: item.path, durableBytes: item.durable_bytes })
    }
    cursor = page.next_cursor ?? null
  } while (cursor)
  return streams
}

// PATCHes each stream's idle time, starting at most `rate` requests a second.
export async function setIdleTimes({ baseUrl, streams, idle, rate, out = console.log, err = console.error }) {
  const interval = 1000 / rate
  let next = Date.now()
  let lastProgress = Date.now()
  let changed = 0
  const failed = []
  for (const [index, stream] of streams.entries()) {
    const wait = next - Date.now()
    if (wait > 0) await sleep(wait)
    next = Math.max(next, Date.now()) + interval
    const { response, error, attempts } = await request(`${baseUrl}${stream.path}`, {
      method: `PATCH`,
      headers: { 'stream-tier-after-idle': idle },
    })
    if (response?.ok) {
      changed += 1
    } else {
      const reason = `${describe(response, error)} after ${attempts} attempt${attempts === 1 ? `` : `s`}`
      failed.push({ path: stream.path, reason })
      err(`failed: ${stream.path}: ${reason}`)
    }
    if (Date.now() - lastProgress >= PROGRESS_EVERY_MS) {
      lastProgress = Date.now()
      out(`progress: ${index + 1}/${streams.length}, ${changed} changed, ${failed.length} failed`)
    }
  }
  return { changed, failed }
}

export async function main(argv) {
  let options
  try {
    options = parseOptions(argv)
  } catch (error) {
    console.error(`${error.message}\n${USAGE}`)
    return 2
  }
  const { baseUrl, prefix, idle, commit, rate } = options
  let streams
  try {
    streams = await listStreams({ baseUrl, prefix })
  } catch (error) {
    console.error(error.message)
    return 1
  }
  const bytes = streams.reduce((sum, stream) => sum + stream.durableBytes, 0)
  console.log(`${plural(streams.length, `stream`)} under ${prefix}, ${plural(bytes, `byte`)}`)
  if (!commit) {
    console.log(`dry run: would set Stream-Tier-After-Idle: ${idle} on each; nothing changed. Run again with --commit to apply.`)
    return 0
  }
  console.log(`setting Stream-Tier-After-Idle: ${idle} at ${rate} a second`)
  const { changed, failed } = await setIdleTimes({ baseUrl, streams, idle, rate })
  console.log(`done: ${changed} changed, ${failed.length} failed of ${plural(streams.length, `stream`)}`)
  return failed.length ? 1 : 0
}

function parseOptions(argv) {
  const { values } = parseArgs({
    args: argv,
    options: {
      'base-url': { type: `string` },
      prefix: { type: `string` },
      idle: { type: `string` },
      commit: { type: `boolean`, default: false },
      rate: { type: `string` },
    },
    strict: true,
    allowPositionals: false,
  })
  const base = values[`base-url`]
  if (!base || !/^https?:\/\//.test(base)) throw new Error(`--base-url must be an http(s) URL`)
  if (!values.prefix) throw new Error(`--prefix is required`)
  const idle = values.idle
  // Seconds as the store spells them (no sign, no leading zero), `never`, or
  // `default` to remove the stream's own value.
  if (!idle || !(idle === `never` || idle === `default` || isSeconds(idle))) {
    throw new Error(`--idle must be seconds, never or default`)
  }
  const rate = values.rate === undefined ? DEFAULT_RATE : Number(values.rate)
  if (!Number.isFinite(rate) || rate <= 0) throw new Error(`--rate must be a positive number`)
  return {
    baseUrl: base.replace(/\/+$/, ``),
    prefix: values.prefix.startsWith(`/`) ? values.prefix : `/${values.prefix}`,
    idle,
    commit: values.commit,
    rate,
  }
}

function isSeconds(value) {
  return /^(0|[1-9][0-9]*)$/.test(value) && BigInt(value) <= 18446744073709551615n
}

// One request, retried while it fails in transit or answers 5xx or 429.
async function request(url, init) {
  let response = null
  let error = null
  let attempts = 0
  while (attempts < MAX_ATTEMPTS) {
    if (attempts > 0) await sleep(500 * 2 ** (attempts - 1))
    attempts += 1
    try {
      response = await fetch(url, { ...init, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
      error = null
    } catch (caught) {
      response = null
      error = caught
      continue
    }
    if (!response.ok) response.detail = await response.text().catch(() => ``)
    if (response.status < 500 && response.status !== 429) break
  }
  return { response, error, attempts }
}

function describe(response, error) {
  if (!response) return error?.cause?.message ?? error?.message ?? `no response`
  const detail = response.detail ?? ``
  return `${response.status}${detail ? ` ${detail.trim()}` : ``}`
}

function plural(count, noun) {
  return `${count} ${noun}${count === 1 ? `` : `s`}`
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  process.exitCode = await main(process.argv.slice(2))
}
