#!/usr/bin/env node
'use strict';

/**
 * Code-Intel PreToolUse Hook — Auto-injects code intelligence context
 * when an agent is about to Write or Edit a file.
 *
 * Protocol:
 * - Reads JSON from stdin (Claude Code PreToolUse event)
 * - Filters: only acts on Write and Edit tools
 * - Queries RegistryProvider for entity, references, dependencies
 * - Outputs JSON with additionalContext containing <code-intel-context> XML
 * - Silent exit on any error (never blocks the tool call)
 *
 * @module code-intel-pretool-hook
 */

const path = require('path');

/** Tools that trigger code-intel injection. */
const TARGET_TOOLS = new Set(['Write', 'Edit']);

/** Safety timeout (ms) — defense-in-depth. */
const HOOK_TIMEOUT_MS = 4000;

/**
 * Read all data from stdin as a JSON object.
 * @returns {Promise<object>}
 */
function readStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('error', (e) => reject(e));
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => {
      try { resolve(JSON.parse(data)); }
      catch (e) { reject(e); }
    });
  });
}

/**
 * Main hook pipeline.
 */
async function main() {
  const input = await readStdin();

  // Filter: only act on Write/Edit tools
  const toolName = input && input.tool_name;
  if (!toolName || !TARGET_TOOLS.has(toolName)) return;

  // Extract file_path from tool input
  const toolInput = input.tool_input;
  if (!toolInput) return;
  const filePath = toolInput.file_path;
  if (!filePath) return;

  // Resolve project root from hook cwd or process.cwd()
  const cwd = input.cwd || process.cwd();

  // Load hook-runtime (lazy — only when we actually need it)
  const { resolveCodeIntel, formatAsXml } = require(
    path.join(__dirname, '..', '..', '.aios-core', 'core', 'code-intel', 'hook-runtime.js'),
  );

  const intel = await resolveCodeIntel(filePath, cwd);
  const xml = formatAsXml(intel, filePath);
  if (!xml) return;

  // Output in Claude Code hook format
  const output = JSON.stringify({
    hookSpecificOutput: {
      additionalContext: xml,
    },
  });

  const flushed = process.stdout.write(output);
  if (!flushed) {
    await new Promise((resolve) => process.stdout.once('drain', resolve));
  }
}

/**
 * Safely exit — no-op inside Jest workers.
 * @param {number} code
 */
function safeExit(code) {
  if (process.env.JEST_WORKER_ID) return;
  process.exit(code);
}

/** Entry point runner. */
function run() {
  const timer = setTimeout(() => safeExit(0), HOOK_TIMEOUT_MS);
  timer.unref();
  main()
    .then(() => safeExit(0))
    .catch(() => {
      // Silent exit — stderr output triggers "hook error" in Claude Code UI
      safeExit(0);
    });
}

if (require.main === module) run();

module.exports = { readStdin, main, run, HOOK_TIMEOUT_MS, TARGET_TOOLS };
