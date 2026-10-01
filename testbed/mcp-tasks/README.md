# MCP Tasks extension testbed

A local MCP server implementing the [Tasks extension](https://github.com/modelcontextprotocol/ext-tasks) (`io.modelcontextprotocol/tasks`) of the 2026-07-28 MCP specification, for testing pi's MCP client.

It is built on `@modelcontextprotocol/server` 2.2.0, which serves both protocol eras (2025 `initialize`, 2026-07-28 `server/discover` with a per-request `_meta` envelope) but does not implement Tasks yet. A small wire layer in `server.ts` answers `tasks/*` and the task tools' `tools/call`; the SDK answers everything else.

## Setup

```bash
cd testbed/mcp-tasks
npm install --ignore-scripts --no-workspaces
```

## Run

```bash
node server.ts              # stdio
node server.ts --http 3939  # Streamable HTTP at http://127.0.0.1:3939/mcp
npm run probe               # raw JSON-RPC walkthrough of every flow, both eras
```

Task transitions are logged to stderr, and appended to `$TESTBED_LOG` when it is set.

## Tools

| Tool | Client without Tasks | Client with Tasks |
| --- | --- | --- |
| `add` | result | result |
| `slow_job` (`seconds`, `fail?: "tool" \| "protocol"`) | blocks, then result | task, polled to `completed` or `failed` |
| `required_task_job` (`seconds`) | error -32021 | task |
| `ask_name` | error -32021 | task that goes `input_required` (elicitation), completes after `tasks/update` |

Over HTTP the server also checks the `MCP-Protocol-Version`, `Mcp-Method`, and `Mcp-Name` headers of the requests it intercepts; the SDK checks the others.

## pi

`packages/coding-agent/test/suite/agent-session-mcp-tasks.test.ts` runs a codemode script against the testbed over stdio and HTTP, with and without `"protocolVersion": "2026-07-28"`. It is skipped until the dependencies above are installed.

To try it in pi, add the server to `mcp.json`:

```json
{
	"mcpServers": {
		"testbed": {
			"command": "node",
			"args": ["/path/to/pi/testbed/mcp-tasks/server.ts"],
			"protocolVersion": "2026-07-28"
		}
	}
}
```
