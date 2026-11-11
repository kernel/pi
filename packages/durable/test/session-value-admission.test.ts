// Host value admission rejects new data before publication without poisoning a Session.
import { fauxAssistantMessage, fauxText, fauxToolCall } from "@earendil-works/pi-ai";
import { createSession, defineDoc, defineTool, Harness } from "@earendil-works/pi-durable";
import { obj } from "@earendil-works/pi-schema";
import { describe, expect, it } from "vitest";
import { chatSetup } from "./chat-support.ts";
import { addTool } from "./harness-support.ts";
import { ControlledStorage, context } from "./session-support.ts";

const State = defineDoc<{ text: string; keys: Record<string, number> }>({
	kind: "test.admission",
	version: 1,
	scope: "conversation",
	history: "latest",
	fork: "current",
	initial: () => ({ text: "", keys: {} }),
	checkpointWhen: () => false,
});

function validateValue(value: unknown): void {
	if (JSON.stringify(value).includes("REJECT")) throw new Error("value rejected");
}

describe("value admission", () => {
	it("rolls back records and document drafts, then accepts another commit", async () => {
		const storage = new ControlledStorage();
		const session = createSession(storage, { validateValue });
		const conversation = await session.commit(
			async (tx) => (await tx.createConversation({ ownership: { kind: "ownerless" } })).id,
			context,
		);
		await session.commit(async (tx) => {
			(await tx.doc(State, conversation)).text = "before";
		}, context);
		const commits = storage.commits.length;
		await expect(
			session.commit(async (tx) => {
				(await tx.doc(State, conversation)).text = "REJECT";
				await tx.appendEntry(conversation, { kind: "test", data: "not stored" });
			}, context),
		).rejects.toThrow("value rejected");
		expect(storage.commits).toHaveLength(commits);
		expect((await session.snapshot(State, conversation, context))?.text).toBe("before");
		await session.commit(async (tx) => {
			(await tx.doc(State, conversation)).text = "after";
		}, context);
		expect((await session.snapshot(State, conversation, context))?.text).toBe("after");
		await session.close(context);
	});

	it("rejects a growing document even when its individual delta passes", async () => {
		const storage = new ControlledStorage();
		const session = createSession(storage, {
			validateValue: (value) => {
				if (
					typeof value === "object" &&
					value !== null &&
					"keys" in value &&
					Object.keys(value.keys as object).length > 2
				)
					throw new Error("document too large");
			},
		});
		const conversation = await session.commit(
			async (tx) => (await tx.createConversation({ ownership: { kind: "ownerless" } })).id,
			context,
		);
		await session.commit(async (tx) => {
			(await tx.doc(State, conversation)).keys = { a: 1, b: 2 };
		}, context);
		await expect(
			session.commit(async (tx) => {
				(await tx.doc(State, conversation)).keys.c = 3;
			}, context),
		).rejects.toThrow("document too large");
		expect((await session.snapshot(State, conversation, context))?.keys).toEqual({ a: 1, b: 2 });
		await session.close(context);
	});

	it("stores validated full values when folded deferred deltas exceed the limit", async () => {
		const Latest = defineDoc<Record<string, number>>({
			kind: "test.admission.deferred",
			version: 1,
			scope: "conversation",
			history: "latest",
			fork: "initial",
			initial: () => ({}),
			checkpointWhen: () => false,
		});
		const storage = new ControlledStorage();
		const session = createSession(storage, {
			persistWithinMs: Infinity,
			validateValue: (value) => {
				if (JSON.stringify(value).length > 300) throw new Error("value too large");
			},
		});
		const conversation = await session.commit(
			async (tx) => (await tx.createConversation({ ownership: { kind: "ownerless" } })).id,
			context,
		);
		await session.commit(async (tx) => {
			await tx.doc(Latest, conversation);
		}, context);
		for (let i = 0; i < 20; i++) {
			await session.commit(
				async (tx) => {
					const doc = await tx.doc(Latest, conversation);
					delete doc[`previous-long-key-${i - 1}`];
					doc[`previous-long-key-${i}`] = i;
				},
				context,
				{ persistence: { kind: "deferred" } },
			);
		}
		await session.close(context);
		const changes = storage.commits.flat().filter((write) => write.type === "document.change");
		expect(changes).toHaveLength(1);
		expect(changes[0]!.content.kind).toBe("base");
	});

	it("faults a rejected tool result once and keeps the conversation usable after reopen", async () => {
		const setup = chatSetup();
		const storage = new ControlledStorage();
		let executions = 0;
		addTool(
			setup.registry,
			defineTool({
				name: "oversized",
				description: "Return a rejected result",
				parameters: obj({}),
				replay: "safe",
				execute: async () => {
					executions++;
					return { output: [{ type: "text", text: "REJECT" }] };
				},
			}),
		);
		setup.faux.setResponses([
			fauxAssistantMessage([fauxToolCall("oversized", {}, { id: "one" })], { stopReason: "toolUse" }),
			fauxAssistantMessage([fauxText("next")]),
		]);
		const options = { models: setup.models, registry: setup.registry, validateValue };
		const harness = await Harness.open(storage, options, context);
		const root = await harness.root(context, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
		await (await root.submit({ type: "input", content: "run" }, context)).wait(context);
		await root.waitForIdle(context);
		expect(executions).toBe(1);
		await harness.close(context);
		setup.faux.setResponses([fauxAssistantMessage([fauxText("follow-up answer")])]);
		const reopened = await Harness.open(storage.reopen(), options, context);
		const restored = await reopened.root(context);
		expect(
			(await (await restored.submit({ type: "input", content: "follow up" }, context)).wait(context)).status,
		).toBe("done");
		expect(executions).toBe(1);
		await reopened.close(context);
	});
});
