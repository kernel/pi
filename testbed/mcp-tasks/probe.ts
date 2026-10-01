/**
 * Raw JSON-RPC probe for server.ts over stdio. Prints every message on the wire.
 *
 * Usage: node probe.ts modern   # server/discover + tasks extension flow
 *        node probe.ts legacy   # 2025-11-25 initialize flow, no tasks
 */
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

const era = process.argv[2] ?? "modern";
const child = spawn(process.execPath, [new URL("./server.ts", import.meta.url).pathname], {
	stdio: ["pipe", "pipe", "inherit"],
});
const pending = new Map<number, (message: Record<string, unknown>) => void>();
let nextId = 1;

createInterface({ input: child.stdout }).on("line", (line) => {
	console.log(`<- ${line}`);
	const message = JSON.parse(line);
	if (message.id !== undefined && pending.has(message.id)) {
		pending.get(message.id)?.(message);
		pending.delete(message.id);
	}
});

const envelope = (withTasks: boolean) => ({
	"io.modelcontextprotocol/protocolVersion": "2026-07-28",
	"io.modelcontextprotocol/clientInfo": { name: "probe", version: "0.0.0" },
	"io.modelcontextprotocol/clientCapabilities": withTasks ? { extensions: { "io.modelcontextprotocol/tasks": {} } } : {},
});

function request(method: string, params: Record<string, unknown> = {}, withTasks = true): Promise<Record<string, unknown>> {
	const id = nextId++;
	const body = era === "modern" ? { ...params, _meta: { ...envelope(withTasks), ...(params._meta as object) } } : params;
	const line = JSON.stringify({ jsonrpc: "2.0", id, method, params: body });
	console.log(`-> ${line}`);
	child.stdin.write(`${line}\n`);
	return new Promise((resolve) => pending.set(id, resolve));
}

function notify(method: string): void {
	const line = JSON.stringify({ jsonrpc: "2.0", method });
	console.log(`-> ${line}`);
	child.stdin.write(`${line}\n`);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function pollTask(taskId: string): Promise<Record<string, unknown>> {
	for (;;) {
		const { result } = (await request("tasks/get", { taskId })) as { result: Record<string, unknown> };
		if (result.status === "input_required") {
			await request("tasks/update", { taskId, inputResponses: { name: { action: "accept", content: { name: "Raf" } } } });
		} else if (["completed", "failed", "cancelled"].includes(result.status as string)) {
			return result;
		}
		await sleep((result.pollIntervalMs as number) ?? 500);
	}
}

if (era === "legacy") {
	await request("initialize", {
		protocolVersion: "2025-11-25",
		capabilities: {},
		clientInfo: { name: "probe", version: "0.0.0" },
	});
	notify("notifications/initialized");
	await request("tools/list");
	await request("tools/call", { name: "add", arguments: { a: 1, b: 2 } });
	await request("tools/call", { name: "slow_job", arguments: { seconds: 1 } });
	await request("tools/call", { name: "required_task_job", arguments: { seconds: 1 } });
} else {
	await request("server/discover");
	await request("tools/list");
	await request("tools/call", { name: "add", arguments: { a: 1, b: 2 } });
	console.log("\n## slow_job without the tasks capability: runs inline");
	await request("tools/call", { name: "slow_job", arguments: { seconds: 1 } }, false);
	console.log("\n## required_task_job without the tasks capability: -32021");
	await request("tools/call", { name: "required_task_job", arguments: { seconds: 1 } }, false);
	console.log("\n## slow_job with the tasks capability: task handle, then poll");
	const created = (await request("tools/call", { name: "slow_job", arguments: { seconds: 1 } })) as {
		result: { taskId: string };
	};
	await pollTask(created.result.taskId);
	console.log("\n## slow_job ending in a protocol error: failed task");
	const failing = (await request("tools/call", { name: "slow_job", arguments: { seconds: 1, fail: "protocol" } })) as {
		result: { taskId: string };
	};
	await pollTask(failing.result.taskId);
	console.log("\n## ask_name: input_required, tasks/update, completed");
	const asking = (await request("tools/call", { name: "ask_name", arguments: {} })) as { result: { taskId: string } };
	await pollTask(asking.result.taskId);
	console.log("\n## tasks/cancel");
	const cancelling = (await request("tools/call", { name: "slow_job", arguments: { seconds: 30 } })) as {
		result: { taskId: string };
	};
	await request("tasks/cancel", { taskId: cancelling.result.taskId });
	await request("tasks/get", { taskId: cancelling.result.taskId });
}
child.kill();
