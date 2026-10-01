/**
 * MCP test server for the Tasks extension (`io.modelcontextprotocol/tasks`, 2026-07-28 spec).
 *
 * The official SDK (@modelcontextprotocol/server v2) serves both protocol eras: the 2025-11-25
 * `initialize` handshake and the 2026-07-28 `server/discover` + per-request `_meta` envelope.
 * It does not implement the Tasks extension yet (typescript-sdk#2189), and 2.2.0 rejects
 * `tasks/*` methods in the 2026 era and wraps thrown tool errors in `isError` results. So a thin
 * wire layer in front of the SDK (`interceptTasks`) answers `tasks/*` and `tools/call` for the
 * task tools; the SDK answers everything else, including `tools/list` for every tool.
 *
 * Tools:
 * - `add`: always synchronous. Control case.
 * - `slow_job`: task when the request declares the tasks extension, otherwise blocks until done.
 *   `fail: "tool"` ends in an `isError` result, `fail: "protocol"` in a JSON-RPC error (a
 *   `failed` task).
 * - `required_task_job`: task only. Without the capability: JSON-RPC error -32021.
 * - `ask_name`: task that moves to `input_required` with an elicitation input request, and
 *   completes after `tasks/update`. Task only.
 * - `confirm_delete`: not a task. On 2026-07-28 it first answers with an `input_required` result
 *   (a multi-round-trip request asking for confirmation) and completes when the client retries with
 *   `inputResponses`. 2025-era clients get an inline result.
 *
 * Usage: node server.ts             stdio
 *        node server.ts --http 3939 Streamable HTTP at http://127.0.0.1:3939/mcp
 * Task transitions and intercepted traffic are logged to stderr, and appended to $TESTBED_LOG
 * when set.
 */
import { randomUUID } from "node:crypto";
import { appendFileSync } from "node:fs";
import { createServer } from "node:http";
import { type CallToolResult, createMcpHandler, McpServer, type McpRequestContext } from "@modelcontextprotocol/server";
import { serveStdio, StdioServerTransport } from "@modelcontextprotocol/server/stdio";
import * as z from "zod/v4";

const TASKS = "io.modelcontextprotocol/tasks";
const PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion";
const CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities";
const SERVER_INFO = { name: "tasks-testbed", version: "0.2.0" };
const POLL_INTERVAL_MS = 500;
const TTL_MS = 10 * 60_000;

function log(line: string): void {
	process.stderr.write(`[testbed] ${line}\n`);
	if (process.env.TESTBED_LOG) appendFileSync(process.env.TESTBED_LOG, `${new Date().toISOString()} ${line}\n`);
}

// ---------------------------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------------------------

type Status = "working" | "input_required" | "completed" | "failed" | "cancelled";
type JsonRpcError = { code: number; message: string; data?: unknown };

interface TaskRecord {
	taskId: string;
	status: Status;
	statusMessage?: string;
	createdAt: string;
	lastUpdatedAt: string;
	result?: CallToolResult;
	error?: JsonRpcError;
	inputRequests?: Record<string, unknown>;
	onInput?: (responses: Record<string, unknown>) => void;
	timer?: ReturnType<typeof setTimeout>;
}

const tasks = new Map<string, TaskRecord>();

const isTerminal = (status: Status) => status === "completed" || status === "failed" || status === "cancelled";

function update(task: TaskRecord, patch: Partial<TaskRecord>): void {
	if (isTerminal(task.status)) return;
	Object.assign(task, patch, { lastUpdatedAt: new Date().toISOString() });
	log(`task ${task.taskId} -> ${task.status}${task.statusMessage ? ` (${task.statusMessage})` : ""}`);
}

function createTask(statusMessage: string): TaskRecord {
	const createdAt = new Date().toISOString();
	const task: TaskRecord = { taskId: randomUUID(), status: "working", statusMessage, createdAt, lastUpdatedAt: createdAt };
	tasks.set(task.taskId, task);
	log(`task ${task.taskId} created (${statusMessage})`);
	return task;
}

/** `resultType: "task"` makes it a CreateTaskResult; `"complete"` a GetTaskResult (DetailedTask). */
function taskView(task: TaskRecord, resultType: "task" | "complete"): Record<string, unknown> {
	return {
		resultType,
		taskId: task.taskId,
		status: task.status,
		...(task.statusMessage === undefined ? {} : { statusMessage: task.statusMessage }),
		createdAt: task.createdAt,
		lastUpdatedAt: task.lastUpdatedAt,
		ttlMs: TTL_MS,
		pollIntervalMs: POLL_INTERVAL_MS,
		...(task.status === "completed" ? { result: task.result } : {}),
		...(task.status === "failed" ? { error: task.error } : {}),
		...(task.status === "input_required" ? { inputRequests: task.inputRequests } : {}),
		_meta: { "io.modelcontextprotocol/serverInfo": SERVER_INFO },
	};
}

const text = (value: string, isError = false): CallToolResult => ({
	content: [{ type: "text", text: value }],
	...(isError ? { isError: true } : {}),
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** What `slow_job` ends in after its work. */
function slowJobOutcome(seconds: number, fail: unknown): { result: CallToolResult } | { error: JsonRpcError } {
	if (fail === "protocol") return { error: { code: -32603, message: "Simulated protocol failure" } };
	if (fail === "tool") return { result: text(`slow_job failed after ${seconds}s`, true) };
	return { result: text(`slow_job finished after ${seconds}s`) };
}

function startSlowTask(seconds: number, fail: unknown): TaskRecord {
	const task = createTask(`working for ${seconds}s`);
	task.timer = setTimeout(() => {
		const outcome = slowJobOutcome(seconds, fail);
		if ("error" in outcome) update(task, { status: "failed", statusMessage: outcome.error.message, error: outcome.error });
		else update(task, { status: "completed", statusMessage: "done", result: outcome.result });
	}, seconds * 1000);
	return task;
}

function startAskNameTask(): TaskRecord {
	const task = createTask("preparing question");
	task.timer = setTimeout(() => {
		update(task, {
			status: "input_required",
			statusMessage: "waiting for name",
			inputRequests: {
				name: {
					method: "elicitation/create",
					params: {
						mode: "form",
						message: "Please enter your name.",
						requestedSchema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] },
					},
				},
			},
		});
		task.onInput = (responses) => {
			const response = responses.name as { action?: string; content?: { name?: unknown } } | undefined;
			if (!response) return;
			task.onInput = undefined;
			update(task, { status: "working", statusMessage: "greeting", inputRequests: undefined });
			task.timer = setTimeout(() => {
				const name = response.action === "accept" ? response.content?.name : undefined;
				update(task, {
					status: "completed",
					statusMessage: "done",
					result: typeof name === "string" ? text(`Hello, ${name}!`) : text(`No name given (${response.action})`, true),
				});
			}, 500);
		};
	}, 500);
	return task;
}

// ---------------------------------------------------------------------------------------------
// Wire layer
// ---------------------------------------------------------------------------------------------

type JsonRpcRequest = { jsonrpc: "2.0"; id: string | number; method: string; params?: Record<string, unknown> };
type JsonRpcResponse = { jsonrpc: "2.0"; id: string | number } & ({ result: unknown } | { error: JsonRpcError });

const INTERCEPTED_TOOLS = new Set(["slow_job", "required_task_job", "ask_name", "confirm_delete"]);

const missingCapability: JsonRpcError = {
	code: -32021,
	message: "Missing required client capability",
	data: { requiredCapabilities: { extensions: { [TASKS]: {} } } },
};

/** Whether a 2026-era request declares the tasks extension in its per-request capabilities. */
function declaresTasks(params: Record<string, unknown> | undefined): boolean {
	const meta = params?._meta as Record<string, unknown> | undefined;
	if (typeof meta?.[PROTOCOL_VERSION] !== "string") return false;
	const capabilities = meta[CLIENT_CAPABILITIES] as { extensions?: Record<string, unknown> } | undefined;
	return capabilities?.extensions?.[TASKS] !== undefined;
}

function isRequest(message: unknown): message is JsonRpcRequest {
	const value = message as Partial<JsonRpcRequest> | undefined;
	return typeof value?.method === "string" && value.id !== undefined;
}

/**
 * Answers the requests the SDK cannot: `tasks/*` and `tools/call` of the task tools. Returns
 * undefined for every other message, which then goes to the SDK.
 */
function interceptTasks(message: unknown): Promise<JsonRpcResponse> | undefined {
	if (!isRequest(message)) return undefined;
	const { id, method, params } = message;
	const toolName = method === "tools/call" ? params?.name : undefined;
	if (!method.startsWith("tasks/") && !(typeof toolName === "string" && INTERCEPTED_TOOLS.has(toolName))) return undefined;
	log(`intercepted ${method}${toolName ? ` ${toolName}` : ""} (tasks declared: ${declaresTasks(params)})`);
	const respond = async (): Promise<JsonRpcResponse> => {
		try {
			return { jsonrpc: "2.0", id, result: await answer(method, toolName as string | undefined, params ?? {}) };
		} catch (error) {
			return { jsonrpc: "2.0", id, error: error as JsonRpcError };
		}
	};
	return respond();
}

async function answer(method: string, toolName: string | undefined, params: Record<string, unknown>): Promise<unknown> {
	const withTasks = declaresTasks(params);
	if (method === "tools/call") {
		const args = (params.arguments ?? {}) as Record<string, unknown>;
		const modern = typeof (params._meta as Record<string, unknown> | undefined)?.[PROTOCOL_VERSION] === "string";
		if (toolName === "confirm_delete") {
			if (!modern) return text("deleted (2025-era clients are not asked to confirm)");
			const response = (params.inputResponses as Record<string, { action?: string }> | undefined)?.confirm;
			if (!response) {
				return {
					resultType: "input_required",
					inputRequests: {
						confirm: {
							method: "elicitation/create",
							params: {
								mode: "form",
								message: "Really delete?",
								requestedSchema: { type: "object", properties: {} },
							},
						},
					},
				};
			}
			return { resultType: "complete", ...text(response.action === "accept" ? "deleted" : "kept", false) };
		}
		const seconds = typeof args.seconds === "number" ? args.seconds : 3;
		if (toolName === "slow_job") {
			if (withTasks) return taskView(startSlowTask(seconds, args.fail), "task");
			await sleep(seconds * 1000);
			const outcome = slowJobOutcome(seconds, args.fail);
			if ("error" in outcome) throw outcome.error;
			return modern ? { resultType: "complete", ...outcome.result } : outcome.result;
		}
		if (!withTasks) throw missingCapability;
		return taskView(toolName === "ask_name" ? startAskNameTask() : startSlowTask(seconds, undefined), "task");
	}
	if (!withTasks) throw missingCapability;
	const task = tasks.get(params.taskId as string);
	if (!task) throw { code: -32602, message: "Failed to retrieve task: Task not found" };
	switch (method) {
		case "tasks/get":
			return taskView(task, "complete");
		case "tasks/update":
			task.onInput?.((params.inputResponses ?? {}) as Record<string, unknown>);
			return { resultType: "complete" };
		case "tasks/cancel":
			clearTimeout(task.timer);
			update(task, { status: "cancelled", statusMessage: "cancelled by client" });
			return { resultType: "complete" };
		default:
			throw { code: -32601, message: "Method not found" };
	}
}

/**
 * The Streamable HTTP routing headers of the 2026-07-28 revision, which the SDK checks only for the
 * requests it serves: `MCP-Protocol-Version` and `Mcp-Method` on every request, and `Mcp-Name`
 * mirroring `params.name` (tools/call) or `params.taskId` (tasks/*). Legacy requests carry neither.
 */
function checkRoutingHeaders(message: JsonRpcRequest, headers: Record<string, string | string[] | undefined>): string | undefined {
	const version = (message.params?._meta as Record<string, unknown> | undefined)?.[PROTOCOL_VERSION];
	if (typeof version !== "string") return undefined;
	if (headers["mcp-protocol-version"] !== version) return `MCP-Protocol-Version header must be ${version}`;
	if (headers["mcp-method"] !== message.method) return `Mcp-Method header must be ${message.method}`;
	const name = message.method === "tools/call" ? message.params?.name : message.params?.taskId;
	if (headers["mcp-name"] !== name) return `Mcp-Name header must be ${String(name)}`;
	return undefined;
}

/** stdio transport that lets `interceptTasks` answer before the SDK sees a message. */
class TaskInterceptingTransport {
	private readonly inner = new StdioServerTransport();
	onmessage?: (message: unknown, extra?: unknown) => void;
	onclose?: () => void;
	onerror?: (error: Error) => void;

	async start(): Promise<void> {
		this.inner.onmessage = (message, extra) => {
			const response = interceptTasks(message);
			if (response) void response.then((value) => this.inner.send(value as never));
			else this.onmessage?.(message, extra);
		};
		this.inner.onclose = () => this.onclose?.();
		this.inner.onerror = (error) => this.onerror?.(error);
		await this.inner.start();
	}

	send(...args: Parameters<StdioServerTransport["send"]>): Promise<void> {
		return this.inner.send(...args);
	}

	close(): Promise<void> {
		return this.inner.close();
	}
}

// ---------------------------------------------------------------------------------------------
// SDK server: handshake, tools/list, and the synchronous tools
// ---------------------------------------------------------------------------------------------

function buildServer(ctx: McpRequestContext): McpServer {
	const server = new McpServer(SERVER_INFO, {
		capabilities: { tools: {}, ...(ctx.era === "modern" ? { extensions: { [TASKS]: {} } } : {}) },
		instructions: "Test server for the MCP Tasks extension (io.modelcontextprotocol/tasks).",
	});
	server.registerTool(
		"add",
		{ description: "Adds two numbers. Always synchronous.", inputSchema: z.object({ a: z.number(), b: z.number() }) },
		async ({ a, b }) => text(String(a + b)),
	);
	// Declared here for tools/list; `interceptTasks` answers their calls.
	const unreachable = async (): Promise<CallToolResult> => text("intercepted before the SDK", true);
	server.registerTool(
		"slow_job",
		{
			description:
				"Simulates a long job. Returns a task when the client supports the tasks extension, otherwise blocks until done.",
			inputSchema: z.object({
				seconds: z.number().min(0).max(600).default(3),
				fail: z.enum(["tool", "protocol"]).optional().describe("End in a tool error or a protocol error"),
			}),
		},
		unreachable,
	);
	server.registerTool(
		"required_task_job",
		{
			description: "Like slow_job, but only runs as a task. Clients without the tasks extension get error -32021.",
			inputSchema: z.object({ seconds: z.number().min(0).max(600).default(3) }),
		},
		unreachable,
	);
	server.registerTool(
		"confirm_delete",
		{
			description:
				"Asks for confirmation through a multi-round-trip input request (2026-07-28), then reports what happened.",
			inputSchema: z.object({}),
		},
		unreachable,
	);
	server.registerTool(
		"ask_name",
		{
			description: "Asks the user for their name through a task input request (elicitation), then greets them.",
			inputSchema: z.object({}),
		},
		unreachable,
	);
	return server;
}

const onerror = (error: Error) => log(`error: ${error.message}`);
const httpIndex = process.argv.indexOf("--http");
if (httpIndex === -1) {
	serveStdio(buildServer, { transport: new TaskInterceptingTransport() as never, onerror });
	log("serving over stdio");
} else {
	const port = Number(process.argv[httpIndex + 1] ?? 3939);
	const handler = createMcpHandler(buildServer, { onerror });
	createServer(async (req, res) => {
		if (req.url !== "/mcp") {
			res.writeHead(404).end();
			return;
		}
		const chunks: Buffer[] = [];
		for await (const chunk of req) chunks.push(chunk as Buffer);
		const body = Buffer.concat(chunks);
		if (req.method === "POST") {
			let message: unknown;
			try {
				message = JSON.parse(body.toString());
			} catch {
				message = undefined;
			}
			const intercepted = interceptTasks(message);
			if (intercepted) {
				const headerError = checkRoutingHeaders(message as JsonRpcRequest, req.headers);
				if (headerError) {
					log(`rejected: ${headerError}`);
					res.writeHead(400, { "content-type": "application/json" }).end(
						JSON.stringify({ jsonrpc: "2.0", id: (message as JsonRpcRequest).id, error: { code: -32600, message: headerError } }),
					);
					return;
				}
				res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(await intercepted));
				return;
			}
		}
		const headers = new Headers();
		for (const [key, value] of Object.entries(req.headers)) {
			if (typeof value === "string") headers.set(key, value);
		}
		const response = await handler.fetch(
			new Request(`http://127.0.0.1:${port}/mcp`, {
				method: req.method,
				headers,
				body: req.method === "GET" || req.method === "HEAD" ? undefined : body,
			}),
		);
		res.writeHead(response.status, Object.fromEntries(response.headers));
		res.end(Buffer.from(await response.arrayBuffer()));
	}).listen(port, "127.0.0.1", () => log(`listening on http://127.0.0.1:${port}/mcp`));
}
