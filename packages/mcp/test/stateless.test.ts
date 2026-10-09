import { afterEach, describe, expect, it } from "vitest";
import {
	createTasksExtension,
	type JsonRpcMessage,
	type JsonRpcRequest,
	McpAbortError,
	McpClient,
	type McpClientExtension,
	McpError,
	STATELESS_PROTOCOL_VERSION,
	StreamableHttpTransport,
	TASKS_EXTENSION,
} from "../src/index.ts";
import { createInMemoryTransportPair } from "../src/testing/index.ts";
import { closeServers, listen, readBody } from "./helpers.ts";

type Handler = (request: JsonRpcRequest) => unknown;

const CLIENT_INFO = { name: "test-client", version: "2.0.0" };

const DISCOVER_RESULT = {
	resultType: "complete",
	supportedVersions: [STATELESS_PROTOCOL_VERSION],
	capabilities: { tools: {}, extensions: { [TASKS_EXTENSION]: {} } },
	instructions: "Stateless test server.",
	_meta: { "io.modelcontextprotocol/serverInfo": { name: "test-server", version: "1.0.0" } },
};

function task(status: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		taskId: "task-1",
		status,
		createdAt: "2026-10-01T00:00:00Z",
		lastUpdatedAt: "2026-10-01T00:00:00Z",
		ttlMs: null,
		pollIntervalMs: 1,
		...extra,
	};
}

/** In-memory server that answers each method with its handler and records what it received. */
async function connect(
	handlers: Record<string, Handler>,
	extensions: McpClientExtension[] = [createTasksExtension()],
): Promise<{ client: McpClient; messages: JsonRpcMessage[] }> {
	const pair = createInMemoryTransportPair();
	const messages: JsonRpcMessage[] = [];
	const all: Record<string, Handler> = { "server/discover": () => DISCOVER_RESULT, ...handlers };
	pair.server.onMessage((message) => {
		messages.push(message);
		if (!("id" in message) || !("method" in message)) return;
		const request = message as JsonRpcRequest;
		queueMicrotask(async () => {
			const handler = all[request.method];
			try {
				if (!handler) throw new McpError(-32601, `Method not found: ${request.method}`);
				await pair.server.send({ jsonrpc: "2.0", id: request.id, result: await handler(request) });
			} catch (error) {
				const { code, message } = error as McpError;
				await pair.server.send({ jsonrpc: "2.0", id: request.id, error: { code, message } });
			}
		});
	});
	await pair.server.start();
	const client = new McpClient({ ...CLIENT_INFO, protocolVersion: STATELESS_PROTOCOL_VERSION, extensions });
	await client.connect(pair.client);
	return { client, messages };
}

const requests = (messages: JsonRpcMessage[]) =>
	messages.filter((message): message is JsonRpcRequest => "id" in message && "method" in message);

describe("stateless protocol revision", () => {
	afterEach(closeServers);

	it("connects with server/discover and sends the envelope on every request", async () => {
		const { client, messages } = await connect({ "tools/list": () => ({ resultType: "complete", tools: [] }) });
		expect(client.protocolVersion).toBe(STATELESS_PROTOCOL_VERSION);
		expect(client.serverInfo).toEqual({ name: "test-server", version: "1.0.0" });
		expect(client.instructions).toBe("Stateless test server.");
		await client.listTools();

		const envelope = {
			"io.modelcontextprotocol/protocolVersion": STATELESS_PROTOCOL_VERSION,
			"io.modelcontextprotocol/clientInfo": CLIENT_INFO,
			"io.modelcontextprotocol/clientCapabilities": { extensions: { [TASKS_EXTENSION]: {} } },
		};
		// No initialize and no initialized notification.
		expect(messages).toEqual([
			{ jsonrpc: "2.0", id: 1, method: "server/discover", params: { _meta: envelope } },
			{ jsonrpc: "2.0", id: 2, method: "tools/list", params: { _meta: envelope } },
		]);
	});

	it("rejects servers that do not offer the revision", async () => {
		await expect(
			connect({ "server/discover": () => ({ ...DISCOVER_RESULT, supportedVersions: ["2027-01-01"] }) }),
		).rejects.toThrow("does not support protocol version 2026-07-28 (supports 2027-01-01)");
	});

	it("does not declare extensions on initialize connections", async () => {
		const pair = createInMemoryTransportPair();
		const messages: JsonRpcMessage[] = [];
		pair.server.onMessage((message) => {
			messages.push(message);
			if ("id" in message && "method" in message && message.method === "initialize") {
				const result = { protocolVersion: "2025-11-25", capabilities: {}, serverInfo: { name: "s", version: "1" } };
				queueMicrotask(() => void pair.server.send({ jsonrpc: "2.0", id: message.id, result }));
			}
		});
		await pair.server.start();
		const client = new McpClient({ ...CLIENT_INFO, extensions: [createTasksExtension()] });
		await client.connect(pair.client);
		expect((messages[0] as JsonRpcRequest).params).toMatchObject({ capabilities: {} });
	});

	it("rejects input_required results instead of resolving to an empty result", async () => {
		const inputRequired = {
			resultType: "input_required",
			inputRequests: { confirm: { method: "elicitation/create" } },
		};
		const { client } = await connect({ "tools/call": () => inputRequired, "resources/read": () => inputRequired });
		await expect(client.callTool("delete", {})).rejects.toThrow(
			"MCP server needs input to answer tools/call (confirm), which this client does not support",
		);
		await expect(client.readResource("docs://a")).rejects.toThrow("needs input to answer resources/read (confirm)");
	});

	it("rejects result types no extension claims", async () => {
		const { client } = await connect({ "tools/call": () => ({ resultType: "task", ...task("working") }) }, []);
		await expect(client.callTool("slow", {})).rejects.toThrow("Unexpected MCP tools/call result type task");
	});

	it("hands claimed results to the extension and returns what it resolves to", async () => {
		const seen: unknown[] = [];
		const extension: McpClientExtension = {
			id: "com.example/receipts",
			settings: { version: 1 },
			claims: {
				receipt: async (result, context) => {
					seen.push(result.receiptId, context.client instanceof McpClient);
					return { content: [{ type: "text", text: "redeemed" }] };
				},
			},
		};
		const { client, messages } = await connect(
			{ "tools/call": () => ({ resultType: "receipt", receiptId: "r-1" }) },
			[extension],
		);
		expect(await client.callTool("buy", {})).toEqual({ content: [{ type: "text", text: "redeemed" }] });
		expect(seen).toEqual(["r-1", true]);
		expect(requests(messages)[0]?.params).toMatchObject({
			_meta: {
				"io.modelcontextprotocol/clientCapabilities": { extensions: { "com.example/receipts": { version: 1 } } },
			},
		});
	});
});

describe("Tasks extension", () => {
	it("polls a task until it completes and resolves to its result", async () => {
		const states = [
			task("working", { statusMessage: "halfway" }),
			task("completed", { result: { content: [{ type: "text", text: "done" }] } }),
		];
		const { client, messages } = await connect({
			"tools/call": () => ({ resultType: "task", ...task("working") }),
			"tasks/get": () => ({ resultType: "complete", ...states.shift() }),
		});
		const progress: (string | undefined)[] = [];
		const result = await client.callTool("slow", {}, { onProgress: (update) => progress.push(update.message) });
		expect(result).toEqual({ content: [{ type: "text", text: "done" }] });
		expect(requests(messages).map((request) => request.method)).toEqual([
			"server/discover",
			"tools/call",
			"tasks/get",
			"tasks/get",
		]);
		expect(requests(messages)[2]?.params).toMatchObject({ taskId: "task-1" });
		expect(progress).toEqual([undefined, "halfway"]);
	});

	it("rejects with the JSON-RPC error of a failed task", async () => {
		const { client } = await connect({
			"tools/call": () => ({ resultType: "task", ...task("failed", { error: { code: -32603, message: "boom" } }) }),
		});
		await expect(client.callTool("slow", {})).rejects.toMatchObject({ code: -32603, message: "boom" });
	});

	it("cancels a task that needs input", async () => {
		const { client, messages } = await connect({
			"tools/call": () => ({ resultType: "task", ...task("input_required", { inputRequests: { name: {} } }) }),
			"tasks/cancel": () => ({ resultType: "complete" }),
		});
		await expect(client.callTool("ask", {})).rejects.toThrow("MCP task task-1 needs input (name)");
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(requests(messages).at(-1)).toMatchObject({ method: "tasks/cancel", params: { taskId: "task-1" } });
	});

	it("cancels the task when the caller aborts", async () => {
		const { client, messages } = await connect({
			"tools/call": () => ({ resultType: "task", ...task("working", { pollIntervalMs: 60_000 }) }),
			"tasks/cancel": () => ({ resultType: "complete" }),
		});
		const controller = new AbortController();
		const call = client.callTool("slow", {}, { signal: controller.signal });
		await new Promise((resolve) => setTimeout(resolve, 10));
		controller.abort();
		await expect(call).rejects.toBeInstanceOf(McpAbortError);
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(requests(messages).at(-1)).toMatchObject({ method: "tasks/cancel", params: { taskId: "task-1" } });
	});
});

describe("stateless Streamable HTTP", () => {
	afterEach(closeServers);

	it("sends MCP-Protocol-Version, Mcp-Method, and Mcp-Name", async () => {
		const seen: Record<string, string | undefined>[] = [];
		const url = await listen(async (request, response) => {
			const body = JSON.parse(await readBody(request)) as JsonRpcRequest;
			seen.push({
				method: body.method,
				version: request.headers["mcp-protocol-version"] as string | undefined,
				mcpMethod: request.headers["mcp-method"] as string | undefined,
				mcpName: request.headers["mcp-name"] as string | undefined,
			});
			const result = body.method === "server/discover" ? DISCOVER_RESULT : { resultType: "complete", content: [] };
			response.setHeader("content-type", "application/json");
			response.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, result }));
		});
		const client = new McpClient({ ...CLIENT_INFO, protocolVersion: STATELESS_PROTOCOL_VERSION });
		await client.connect(new StreamableHttpTransport({ url: `${url}/mcp`, openGetStream: false }));
		await client.callTool("search", {});
		await client.callTool("café", {});
		expect(seen).toEqual([
			{ method: "server/discover", version: "2026-07-28", mcpMethod: "server/discover", mcpName: undefined },
			{ method: "tools/call", version: "2026-07-28", mcpMethod: "tools/call", mcpName: "search" },
			{ method: "tools/call", version: "2026-07-28", mcpMethod: "tools/call", mcpName: "=?base64?Y2Fmw6k=?=" },
		]);
	});
});
