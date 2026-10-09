/**
 * End-to-end runs of pi's MCP client against the Tasks extension testbed server
 * (`testbed/mcp-tasks/server.ts`) over a real stdio transport. A faux model issues one codemode
 * script that calls every testbed tool and reports what each call resolved or rejected with.
 *
 * Requires `npm install --ignore-scripts --no-workspaces` in `testbed/mcp-tasks`; skipped otherwise.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCodemodeExtension } from "../../src/extensions/codemode/index.ts";
import type { McpServerConfig, McpServerEntry } from "../../src/extensions/mcp/config.ts";
import { createMcpExtension } from "../../src/extensions/mcp/index.ts";
import { createHarness, createTestUiContext, getToolResult, type Harness } from "./harness.ts";

const TESTBED = fileURLToPath(new URL("../../../../testbed/mcp-tasks/", import.meta.url));
const installed = existsSync(`${TESTBED}node_modules/@modelcontextprotocol/server`);

/** Calls each tool and records `{ ok, value }` or `{ ok: false, error }`, so one failure does not end the script. */
const SCRIPT = `
const calls = {
	add: () => tools.mcp__testbed__add({ a: 1, b: 2 }),
	slow_job: () => tools.mcp__testbed__slow_job({ seconds: 1 }),
	slow_job_tool_error: () => tools.mcp__testbed__slow_job({ seconds: 1, fail: "tool" }),
	slow_job_protocol_error: () => tools.mcp__testbed__slow_job({ seconds: 1, fail: "protocol" }),
	required_task_job: () => tools.mcp__testbed__required_task_job({ seconds: 1 }),
	ask_name: () => tools.mcp__testbed__ask_name({}),
	confirm_delete: () => tools.mcp__testbed__confirm_delete({}),
};
const report = {};
for (const [name, call] of Object.entries(calls)) {
	try {
		const result = await call();
		report[name] = { ok: true, isError: result.isError === true, text: result.content.map((block) => block.text).join("") };
	} catch (error) {
		report[name] = { ok: false, error: error.message };
	}
}
text(JSON.stringify(report));
`;

describe.skipIf(!installed)("MCP Tasks extension testbed", () => {
	const harnesses: Harness[] = [];
	const children: ChildProcess[] = [];

	afterEach(() => {
		while (harnesses.length > 0) harnesses.pop()?.cleanup();
		while (children.length > 0) children.pop()?.kill();
	});

	const stdio: McpServerConfig = { command: process.execPath, args: [`${TESTBED}server.ts`], exposure: "codemode" };

	/** Starts the testbed's HTTP server and resolves to its URL once it listens. */
	async function startHttpServer(): Promise<string> {
		const port = 39_000 + Math.floor(Math.random() * 1000);
		const child = spawn(process.execPath, [`${TESTBED}server.ts`, "--http", String(port)], {
			stdio: ["ignore", "ignore", "pipe"],
		});
		children.push(child);
		await new Promise<void>((resolve, reject) => {
			child.stderr?.on("data", (data: Buffer) => {
				if (data.toString().includes("listening")) resolve();
			});
			child.on("exit", (code) => reject(new Error(`testbed exited with ${code}`)));
		});
		return `http://127.0.0.1:${port}/mcp`;
	}

	async function runScript(config: McpServerConfig): Promise<Record<string, unknown>> {
		const entry: McpServerEntry = { name: "testbed", config, source: "test" };
		const harness = await createHarness({
			extensionFactories: [
				createCodemodeExtension(),
				createMcpExtension({ loadConfig: () => ({ servers: [entry], errors: [] }) }),
			],
		});
		harnesses.push(harness);
		await harness.session.bindExtensions({ uiContext: createTestUiContext() });
		await vi.waitFor(
			() => expect(harness.session.getAllTools().some((tool) => tool.name === "mcp__testbed__add")).toBe(true),
			{ timeout: 10_000 },
		);
		harness.setResponses([
			fauxAssistantMessage([fauxToolCall("codemode", { code: SCRIPT })], { stopReason: "toolUse" }),
			fauxAssistantMessage("done"),
		]);
		await harness.session.prompt("run the testbed tools");
		const result = getToolResult(harness, "codemode");
		const output = result.content.find((block) => block.type === "text" && block.text.startsWith("{"));
		return JSON.parse((output as { text: string }).text);
	}

	it("baseline: legacy handshake, no tasks", async () => {
		const report = await runScript(stdio);
		console.log(JSON.stringify(report, null, 2));
		expect(report.add).toEqual({ ok: true, isError: false, text: "3" });
		// Without the tasks extension the server runs slow_job inline.
		expect(report.slow_job).toEqual({ ok: true, isError: false, text: "slow_job finished after 1s" });
		expect(report.required_task_job).toMatchObject({ ok: false });
		expect(report.ask_name).toMatchObject({ ok: false });
		expect(report.confirm_delete).toMatchObject({
			ok: true,
			text: "deleted (2025-era clients are not asked to confirm)",
		});
	}, 30_000);

	for (const transport of ["stdio", "http"] as const) {
		it(`stateless revision over ${transport}: tasks are polled to completion`, async () => {
			const config: McpServerConfig =
				transport === "stdio"
					? { ...stdio, protocolVersion: "2026-07-28" }
					: { url: await startHttpServer(), exposure: "codemode", protocolVersion: "2026-07-28" };
			const report = await runScript(config);
			console.log(JSON.stringify(report, null, 2));
			expect(report.add).toEqual({ ok: true, isError: false, text: "3" });
			expect(report.slow_job).toEqual({ ok: true, isError: false, text: "slow_job finished after 1s" });
			expect(report.slow_job_tool_error).toEqual({ ok: true, isError: true, text: "slow_job failed after 1s" });
			expect(report.slow_job_protocol_error).toEqual({ ok: false, error: "Simulated protocol failure" });
			expect(report.required_task_job).toEqual({ ok: true, isError: false, text: "slow_job finished after 1s" });
			// pi cannot answer input requests yet: the task is cancelled and the call rejects.
			expect(report.ask_name).toMatchObject({ ok: false, error: expect.stringContaining("needs input (name)") });
			// Neither can it answer multi-round-trip input requests: the call rejects instead of resolving to an empty result.
			expect(report.confirm_delete).toMatchObject({
				ok: false,
				error: expect.stringContaining("needs input to answer tools/call (confirm)"),
			});
		}, 30_000);
	}
});
