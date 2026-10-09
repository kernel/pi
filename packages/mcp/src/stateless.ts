/**
 * The stateless protocol revision (2026-07-28). Clients opt in per connection with
 * `protocolVersion: STATELESS_PROTOCOL_VERSION`; there is no fallback to `initialize`.
 *
 * What changes on the wire, and what this client does about it:
 * - `server/discover` replaces the `initialize` handshake and the `initialized` notification.
 * - Every request carries the client's protocol version, info, and capabilities in its `_meta`
 *   envelope ({@link createEnvelope}).
 * - Streamable HTTP requests carry `Mcp-Method` and `Mcp-Name` ({@link routingHeaders}).
 * - Results carry `resultType`. `complete` (or absent) is the ordinary result. `input_required`
 *   (the server needs elicitation or sampling input before it can answer) is rejected, since this
 *   client cannot provide that input yet. Other types belong to extensions that claim them, such as
 *   `task` for the Tasks extension ({@link resolveResult}).
 *
 * Not supported: `subscriptions/listen`, so list-changed and resource-updated notifications do not
 * arrive; result caching hints (`ttlMs`, `cacheScope`) are ignored.
 */

import type { McpClaimContext, McpClaimResolver, McpClientExtension } from "./extension.ts";
import { isJsonRpcRequest, isObject, JSON_RPC_ERROR_CODES, type JsonRpcMessage, McpError } from "./protocol/jsonrpc.ts";
import type { ClientCapabilities, Implementation, InitializeResult, ServerCapabilities } from "./protocol/types.ts";

export const STATELESS_PROTOCOL_VERSION = "2026-07-28";

/** Result of `server/discover`, the stateless revision's replacement for `initialize`. */
export interface DiscoverResult {
	supportedVersions: string[];
	capabilities: ServerCapabilities;
	instructions?: string;
	_meta?: { "io.modelcontextprotocol/serverInfo"?: Implementation };
}

/** The `_meta` keys every request carries. */
export function createEnvelope(clientInfo: Implementation, capabilities: ClientCapabilities): Record<string, unknown> {
	return {
		"io.modelcontextprotocol/protocolVersion": STATELESS_PROTOCOL_VERSION,
		"io.modelcontextprotocol/clientInfo": clientInfo,
		"io.modelcontextprotocol/clientCapabilities": capabilities,
	};
}

/**
 * Capabilities with the extensions declared, for the envelope. Extensions are declared only on
 * stateless connections: the `initialize` handshake has no per-request capabilities for them.
 */
export function withExtensions(
	capabilities: ClientCapabilities,
	extensions: readonly McpClientExtension[],
): ClientCapabilities {
	if (extensions.length === 0) return capabilities;
	return {
		...capabilities,
		extensions: {
			...capabilities.extensions,
			...Object.fromEntries(extensions.map((extension) => [extension.id, extension.settings ?? {}])),
		},
	};
}

/** Validates a `server/discover` result and returns it in the shape of an `initialize` result. */
export function toInitializeResult(value: unknown): InitializeResult {
	if (
		!isObject(value) ||
		!Array.isArray(value.supportedVersions) ||
		!isObject(value.capabilities) ||
		(value.instructions !== undefined && typeof value.instructions !== "string")
	) {
		throw new McpError(JSON_RPC_ERROR_CODES.invalidRequest, "Invalid MCP server/discover result");
	}
	const result = value as unknown as DiscoverResult;
	if (!result.supportedVersions.includes(STATELESS_PROTOCOL_VERSION)) {
		throw new Error(
			`MCP server does not support protocol version ${STATELESS_PROTOCOL_VERSION} (supports ${result.supportedVersions.join(", ")})`,
		);
	}
	return {
		protocolVersion: STATELESS_PROTOCOL_VERSION,
		capabilities: result.capabilities,
		// Servers should, but need not, identify themselves.
		serverInfo: result._meta?.["io.modelcontextprotocol/serverInfo"] ?? { name: "unknown", version: "unknown" },
		...(result.instructions === undefined ? {} : { instructions: result.instructions }),
	};
}

/**
 * The ordinary result of a request: `complete` results as they are, results of other types resolved
 * by the extension that claims them. `claims` is empty for methods extensions cannot claim.
 */
export async function resolveResult(
	method: string,
	result: unknown,
	claims: ReadonlyMap<string, McpClaimResolver>,
	context: McpClaimContext,
): Promise<unknown> {
	if (!isObject(result)) return result;
	const { resultType } = result;
	if (resultType === undefined || resultType === "complete") return result;
	if (resultType === "input_required") {
		const keys = isObject(result.inputRequests) ? Object.keys(result.inputRequests).join(", ") : "";
		throw new McpError(
			JSON_RPC_ERROR_CODES.internalError,
			`MCP server needs input to answer ${method}${keys ? ` (${keys})` : ""}, which this client does not support`,
		);
	}
	const resolve = typeof resultType === "string" ? claims.get(resultType) : undefined;
	if (!resolve) {
		throw new McpError(
			JSON_RPC_ERROR_CODES.invalidRequest,
			`Unexpected MCP ${method} result type ${String(resultType)}`,
		);
	}
	return resolve(result, context);
}

/** The request field the `Mcp-Name` header mirrors, by method. */
const MCP_NAME_SOURCES: Record<string, string> = {
	"tools/call": "name",
	"prompts/get": "name",
	"resources/read": "uri",
	"tasks/get": "taskId",
	"tasks/update": "taskId",
	"tasks/cancel": "taskId",
};

/**
 * `Mcp-Name` value: header-safe strings as they are, others as `=?base64?<utf-8 base64>?=`.
 * Header-safe means non-empty, no surrounding whitespace, and only visible ASCII, space, or tab.
 */
function encodeHeaderValue(value: string): string {
	const safe =
		value.length > 0 && value === value.trim() && /^[\t\x20-\x7e]*$/.test(value) && !/^=\?base64\?.*\?=$/.test(value);
	return safe ? value : `=?base64?${btoa(String.fromCharCode(...new TextEncoder().encode(value)))}?=`;
}

/** `Mcp-Method` and `Mcp-Name`, which Streamable HTTP requests carry so intermediaries can route them. */
export function routingHeaders(message: JsonRpcMessage): Record<string, string> {
	if (!isJsonRpcRequest(message)) return {};
	const source = MCP_NAME_SOURCES[message.method];
	const name = source !== undefined && isObject(message.params) ? message.params[source] : undefined;
	return {
		"Mcp-Method": message.method,
		...(typeof name === "string" ? { "Mcp-Name": encodeHeaderValue(name) } : {}),
	};
}
