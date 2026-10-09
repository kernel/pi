import type { McpClient, McpRequestOptions } from "./client.ts";

/** What a claim resolver gets: the client, to send follow-up requests, and the original call's options. */
export interface McpClaimContext extends McpRequestOptions {
	client: McpClient;
}

/** Turns a claimed result into an ordinary result, or rejects. */
export type McpClaimResolver = (result: Record<string, unknown>, context: McpClaimContext) => Promise<unknown>;

/**
 * An opt-in protocol extension, such as `createTasksExtension()`. On connections with the
 * stateless protocol revision, the client declares it in every request's capabilities, under
 * `extensions[id]`, and hands the `tools/call` results whose `resultType` it claims to it. On
 * `initialize` connections it is neither declared nor used.
 */
export interface McpClientExtension {
	/** Extension identifier, for example `io.modelcontextprotocol/tasks`. */
	id: string;
	/** Settings declared with the identifier. Default: `{}`. */
	settings?: Record<string, unknown>;
	/** Resolvers for `tools/call` results by `resultType`, each resolving to a `CallToolResult`. */
	claims?: Record<string, McpClaimResolver>;
}
