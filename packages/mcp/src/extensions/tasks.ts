/**
 * The Tasks extension (`io.modelcontextprotocol/tasks`, SEP-2663): a server may answer `tools/call`
 * with a task handle (`resultType: "task"`) instead of the result. This extension claims those
 * results and polls `tasks/get` until the task ends, so `McpClient.callTool()` still resolves to the
 * final `CallToolResult`.
 *
 * - `completed`: resolves to the task's result, including results with `isError`.
 * - `failed`: rejects with the task's JSON-RPC error.
 * - `cancelled`: rejects.
 * - `input_required`: cancels the task and rejects. Answering input requests with `tasks/update`
 *   is not supported yet.
 * - The caller aborts: cancels the task and rejects.
 *
 * Polls follow the task's `pollIntervalMs`. `timeoutMs` of the call applies to each poll, not to the
 * whole task. `onProgress` of the call receives the task's status message after every poll.
 */

import type { McpClaimContext, McpClientExtension } from "../extension.ts";
import { isObject, JSON_RPC_ERROR_CODES, McpAbortError, McpError } from "../protocol/jsonrpc.ts";

export const TASKS_EXTENSION = "io.modelcontextprotocol/tasks";

/** Delay between polls when the task suggests none. */
const DEFAULT_POLL_INTERVAL_MS = 1_000;

export type TaskStatus = "working" | "input_required" | "completed" | "failed" | "cancelled";

const STATUSES: ReadonlySet<string> = new Set<TaskStatus>([
	"working",
	"input_required",
	"completed",
	"failed",
	"cancelled",
]);

/**
 * A task as `tasks/get` returns it. `result` is set when `completed`, `error` when `failed`, and
 * `inputRequests` when `input_required`.
 */
export interface Task {
	taskId: string;
	status: TaskStatus;
	statusMessage?: string;
	createdAt: string;
	lastUpdatedAt: string;
	ttlMs: number | null;
	pollIntervalMs?: number;
	result?: Record<string, unknown>;
	error?: { code: number; message: string; data?: unknown };
	inputRequests?: Record<string, unknown>;
}

function validateTask(value: unknown): Task {
	if (!isObject(value) || typeof value.taskId !== "string" || !STATUSES.has(value.status as string)) {
		throw new McpError(JSON_RPC_ERROR_CODES.invalidRequest, "Invalid MCP task");
	}
	return value as unknown as Task;
}

const isTerminal = (task: Task) =>
	task.status === "completed" || task.status === "failed" || task.status === "cancelled";

/** Resolves after `ms`, or rejects with {@link McpAbortError} when `signal` aborts first. */
function delay(ms: number, signal: AbortSignal | undefined): Promise<void> {
	if (signal?.aborted) return Promise.reject(new McpAbortError());
	return new Promise((resolve, reject) => {
		const onAbort = () => {
			clearTimeout(timer);
			reject(new McpAbortError());
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

async function awaitTask(created: Task, context: McpClaimContext): Promise<unknown> {
	const { client, signal, timeoutMs, onProgress } = context;
	const { taskId } = created;
	let task = created;
	try {
		for (let polls = 0; ; polls++) {
			switch (task.status) {
				case "completed":
					return task.result;
				case "failed":
					throw new McpError(
						task.error?.code ?? JSON_RPC_ERROR_CODES.internalError,
						task.error?.message ?? task.statusMessage ?? `MCP task ${taskId} failed`,
						task.error?.data,
					);
				case "cancelled":
					throw new McpError(
						JSON_RPC_ERROR_CODES.internalError,
						`MCP task ${taskId} was cancelled${task.statusMessage ? `: ${task.statusMessage}` : ""}`,
					);
				case "input_required":
					throw new McpError(
						JSON_RPC_ERROR_CODES.internalError,
						`MCP task ${taskId} needs input (${Object.keys(task.inputRequests ?? {}).join(", ")}), which this client does not support`,
					);
			}
			onProgress?.({ progressToken: taskId, progress: polls, message: task.statusMessage });
			await delay(task.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS, signal);
			task = validateTask(await client.request("tasks/get", { taskId }, { signal, timeoutMs }));
		}
	} catch (error) {
		if (!isTerminal(task)) void client.request("tasks/cancel", { taskId }).catch(() => {});
		throw error;
	}
}

export function createTasksExtension(): McpClientExtension {
	return {
		id: TASKS_EXTENSION,
		claims: { task: (result, context) => awaitTask(validateTask(result), context) },
	};
}
