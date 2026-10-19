import { randomUUID } from "node:crypto";
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { idFromNumber } from "../src/ids.ts";
import {
	applyPostgresMigrations,
	CURRENT_POSTGRES_SCHEMA_VERSION,
	PostgresStorage,
} from "../src/storage/postgres/index.ts";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "../src/types.ts";
import { ROOT_CONVERSATION_ID } from "../src/types.ts";

// Set to a database the tests may create and drop schemas in, e.g. postgres://postgres:pg@localhost:5432/postgres.
const url = process.env.PI_DURABLE_POSTGRES_URL;
const describePostgres = describe.skipIf(url === undefined);
const context = BACKGROUND_CONTEXT;
const schema = `pi_durable_test_${randomUUID().replaceAll("-", "")}`;
const openStorages = new Set<Storage>();
let pool: pg.Pool;

beforeAll(async () => {
	if (url === undefined) return;
	pool = new pg.Pool({ connectionString: url, max: 20 });
	await applyPostgresMigrations(pool, schema);
});

afterEach(async () => {
	for (const storage of openStorages) await storage.close(context);
	openStorages.clear();
});

afterAll(async () => {
	if (url === undefined) return;
	await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
	await pool.end();
});

async function openStorage(session: string = randomUUID()): Promise<PostgresStorage> {
	const storage = await PostgresStorage.open(pool, { session, schema });
	openStorages.add(storage);
	return storage;
}

/** Reopens the Session after every commit, so each later call reads what an earlier process persisted. */
class ReopeningStorage implements Storage {
	private current: PostgresStorage;
	private readonly session: string;
	private closed = false;

	constructor(current: PostgresStorage, session: string) {
		this.current = current;
		this.session = session;
	}

	async commit(writes: readonly StorageWrite[], commitContext: Context): Promise<Seq> {
		if (this.closed) return this.current.commit(writes, commitContext);
		try {
			return await this.current.commit(writes, commitContext);
		} finally {
			await this.current.close(context);
			this.current = await PostgresStorage.open(pool, { session: this.session, schema });
		}
	}

	mintId<I extends Id<string>>(): Promise<I> {
		return this.current.mintId<I>();
	}
	conversation: Storage["conversation"] = (id, readContext) => this.current.conversation(id, readContext);
	scanConversations: Storage["scanConversations"] = (query, limit, cursor, readContext) =>
		this.current.scanConversations(query, limit, cursor, readContext);
	entry(id: EntryId, readContext: Context): ReturnType<Storage["entry"]>;
	entry(conversationId: ConversationId, id: EntryId, readContext: Context): ReturnType<Storage["entry"]>;
	entry(idOrConversationId: EntryId | ConversationId, idOrContext: EntryId | Context, readContext?: Context) {
		if (readContext === undefined) {
			return this.current.entry(idFromNumber<EntryId>(idOrConversationId), idOrContext as Context);
		}
		if (typeof idOrContext !== "number") throw new TypeError("Storage.entry() requires an entry ID");
		return this.current.entry(
			idFromNumber<ConversationId>(idOrConversationId),
			idFromNumber<EntryId>(idOrContext),
			readContext,
		);
	}
	findLatestHeadMarker: Storage["findLatestHeadMarker"] = (conversationId, at, readContext) =>
		this.current.findLatestHeadMarker(conversationId, at, readContext);
	scanEntries: Storage["scanEntries"] = (query, limit, cursor, readContext) =>
		this.current.scanEntries(query, limit, cursor, readContext);
	task: Storage["task"] = (id, readContext) => this.current.task(id, readContext);
	scanTasks: Storage["scanTasks"] = (query, limit, cursor, readContext) =>
		this.current.scanTasks(query, limit, cursor, readContext);
	submission: Storage["submission"] = (id, readContext) => this.current.submission(id, readContext);
	scanSubmissions: Storage["scanSubmissions"] = (query, limit, cursor, readContext) =>
		this.current.scanSubmissions(query, limit, cursor, readContext);
	submissionByRequest: Storage["submissionByRequest"] = (conversationId, requestId, readContext) =>
		this.current.submissionByRequest(conversationId, requestId, readContext);
	findDocument: Storage["findDocument"] = (address, at, readContext) =>
		this.current.findDocument(address, at, readContext);
	document: Storage["document"] = (id, at, readContext) => this.current.document(id, at, readContext);
	scanDocuments: Storage["scanDocuments"] = (query, limit, cursor, readContext) =>
		this.current.scanDocuments(query, limit, cursor, readContext);

	async close(closeContext: Context): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		await this.current.close(closeContext);
	}
}

registerStorageConformance({ describe: describePostgres, expect, it }, "PostgresStorage", async (use) =>
	use(await openStorage()),
);

registerStorageConformance({ describe: describePostgres, expect, it }, "PostgresStorage across reopen", async (use) => {
	const session = randomUUID();
	const storage = new ReopeningStorage(await PostgresStorage.open(pool, { session, schema }), session);
	try {
		await use(storage);
	} finally {
		await storage.close(context);
	}
});

async function createRoot(storage: Storage): Promise<Seq> {
	return storage.commit([{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }], context);
}

async function lockHolder(session: string): Promise<number | undefined> {
	const result = await pool.query<{ pid: number }>(
		`SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 AND granted
			AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, 0)`,
		[`pi-durable:session:${schema}:${session}`],
	);
	return result.rows[0]?.pid;
}

describePostgres("PostgresStorage ownership", () => {
	it("keeps Sessions in one schema independent", async () => {
		const first = await openStorage();
		const second = await openStorage();
		await createRoot(first);
		expect(await first.mintId()).toBe(2);
		expect(await second.mintId()).toBe(2);
		expect(await second.conversation(ROOT_CONVERSATION_ID, context)).toBeUndefined();
		await expect(createRoot(second)).resolves.toBe(1);
	});

	it("rejects a second open of a Session until the first closes", async () => {
		const session = randomUUID();
		const first = await openStorage(session);
		await expect(PostgresStorage.open(pool, { session, schema })).rejects.toThrow("is open in another process");
		await first.close(context);
		const second = await openStorage(session);
		await expect(createRoot(second)).resolves.toBe(1);
	});

	it("stops accepting work when the lock connection drops", async () => {
		const session = randomUUID();
		const storage = await openStorage(session);
		const pid = await lockHolder(session);
		expect(pid).toBeDefined();
		await pool.query("SELECT pg_terminate_backend($1)", [pid]);
		await expect
			.poll(() =>
				storage.mintId().then(
					() => "open",
					(error: Error) => error.message,
				),
			)
			.toContain("lost ownership");
		await expect(createRoot(storage)).rejects.toThrow("lost ownership");
		const replacement = await openStorage(session);
		await expect(createRoot(replacement)).resolves.toBe(1);
	});

	it("rejects commits after another process takes ownership", async () => {
		const session = randomUUID();
		const storage = await openStorage(session);
		await createRoot(storage);
		await pool.query(`UPDATE "${schema}".sessions SET owner = 'another-process' WHERE session_id = $1`, [session]);
		const entryId = await storage.mintId<EntryId>();
		await expect(
			storage.commit(
				[{ type: "entry", value: { id: entryId, conversationId: ROOT_CONVERSATION_ID, kind: "message" } }],
				context,
			),
		).rejects.toThrow("was opened by another process");
		await expect(storage.conversation(ROOT_CONVERSATION_ID, context)).rejects.toThrow("lost ownership");
	});

	it("persists the ID and commit sequence across reopen", async () => {
		const session = randomUUID();
		const first = await openStorage(session);
		await createRoot(first);
		const minted = await first.mintId<EntryId>();
		await first.commit(
			[{ type: "entry", value: { id: minted, conversationId: ROOT_CONVERSATION_ID, kind: "message" } }],
			context,
		);
		await first.close(context);
		const second = await openStorage(session);
		expect(await second.mintId()).toBe(minted + 1);
		await expect(createRoot(second)).rejects.toThrow("already belongs to conversation");
		const entryId = await second.mintId<EntryId>();
		await expect(
			second.commit(
				[{ type: "entry", value: { id: entryId, conversationId: ROOT_CONVERSATION_ID, kind: "message" } }],
				context,
			),
		).resolves.toBe(3);
	});
});

describePostgres("Postgres migrations", () => {
	it("are idempotent and record the current version", async () => {
		await applyPostgresMigrations(pool, schema);
		const result = await pool.query<{ version: number }>(`SELECT version FROM "${schema}".durable_schema`);
		expect(result.rows).toEqual([{ version: CURRENT_POSTGRES_SCHEMA_VERSION }]);
	});

	it("serialize concurrent first opens of a fresh schema", async () => {
		const fresh = `pi_durable_test_${randomUUID().replaceAll("-", "")}`;
		try {
			const storages = await Promise.all(
				Array.from({ length: 4 }, () => PostgresStorage.open(pool, { session: randomUUID(), schema: fresh })),
			);
			for (const storage of storages) await storage.close(context);
		} finally {
			await pool.query(`DROP SCHEMA IF EXISTS "${fresh}" CASCADE`);
		}
	});

	it("reject a schema newer than this package supports", async () => {
		const fresh = `pi_durable_test_${randomUUID().replaceAll("-", "")}`;
		try {
			await applyPostgresMigrations(pool, fresh);
			await pool.query(`UPDATE "${fresh}".durable_schema SET version = 99`);
			await expect(PostgresStorage.open(pool, { session: randomUUID(), schema: fresh })).rejects.toThrow(
				"is newer than supported version",
			);
		} finally {
			await pool.query(`DROP SCHEMA IF EXISTS "${fresh}" CASCADE`);
		}
	});
});
