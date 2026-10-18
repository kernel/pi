import type { Context, JsonValue } from "@earendil-works/chord";
import { apply, type Op } from "@earendil-works/chord/delta";
import { StorageRejected } from "../../errors.ts";
import { idFromNumber, seqFromNumber } from "../../ids.ts";
import type {
	ConversationId,
	ConversationQuery,
	ConversationRecord,
	Cursor,
	DocumentAddress,
	DocumentContent,
	DocumentCreate,
	DocumentId,
	DocumentPoint,
	DocumentQuery,
	DocumentRecord,
	EntryId,
	EntryQuery,
	EntryRecord,
	Id,
	JsonObject,
	Page,
	Seq,
	Storage,
	StorageWrite,
	StoredDocument,
	SubmissionId,
	SubmissionQuery,
	SubmissionRecord,
	TaskId,
	TaskQuery,
	TaskRecord,
} from "../../types.ts";
import type { PostgresPool, PostgresPoolClient, PostgresQueryable, PostgresValue } from "./database.ts";
import { applyPostgresMigrations, DEFAULT_POSTGRES_SCHEMA, quoteIdentifier } from "./migrations.ts";

type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
type TableName = "conversation" | "entry" | "task" | "submission" | "document";
type JsonRow = { readonly record: string };
type EntryJsonRow = { readonly record: string; readonly commit_seq: string };
type MetadataRow = { readonly next_id: string; readonly next_seq: string };
/** A conversation's own entries over one contiguous id range ending at its newest entry, newest-last. */
type EntryWindow = { ids: EntryId[]; complete: boolean };
type DocumentState = { readonly record: string; readonly version: number; value: JsonObject; deltasSinceBase: number };
type DocumentWrites = {
	readonly claims: DocumentId[];
	readonly rows: unknown[];
	/** What each document action means for the caches once the commit succeeds. */
	readonly applied: Map<
		DocumentId,
		{
			readonly created: DocumentRecord | undefined;
			readonly retired: boolean;
			readonly content: DocumentContent | undefined;
		}
	>;
};

/** SQLSTATEs the commit function raises, each for one adapter error. */
const commitErrorCodes: Readonly<Record<string, true>> = {
	DU001: true,
	DU002: true,
	DU003: true,
	DU004: true,
	DU005: true,
	DU006: true,
	DU007: true,
	DU008: true,
	DU009: true,
};
type DocumentAction = {
	create?: DocumentCreate;
	copy?: Extract<StorageWrite, { readonly type: "document.copy" }>["source"];
	content?: DocumentContent;
	retire: boolean;
};
type ScopeColumns = {
	readonly scopeKind: DocumentRecord["scope"]["kind"];
	readonly ownerId: number;
};
type Tables = {
	readonly sessions: string;
	readonly recordIds: string;
	readonly conversations: string;
	readonly entries: string;
	readonly tasks: string;
	readonly submissions: string;
	readonly documents: string;
	readonly revisions: string;
};

export interface PostgresStorageOptions {
	/** The Session stored by this storage. Sessions in one schema are independent. */
	readonly session: string;
	/** Schema holding the tables, created on first open. Defaults to `pi_durable`. */
	readonly schema?: string;
	/**
	 * Apply schema migrations on open. Defaults to true. A deployment that migrates in an explicit step before
	 * starting, or opens many Sessions per process, passes false so an open costs two statements and takes no
	 * schema-wide lock.
	 */
	readonly migrate?: boolean;
}

const parseJson = <T>(value: string): T => JSON.parse(value) as T;
const encodeJson = (value: unknown): string => JSON.stringify(value) as string;
// Drivers encode parameters as UTF-8, which replaces lone UTF-16 surrogates. JSON encoding keeps indexed identities
// lossless.
const encodeIndexedString = (value: string): string => JSON.stringify(value);

const cursorId = <I extends Id<string>>(cursor: Cursor | undefined): I | undefined => {
	const after = cursor?.after;
	if (after === undefined) return undefined;
	if (typeof after !== "number" || !Number.isSafeInteger(after)) throw new TypeError("Invalid storage cursor");
	return idFromNumber<I>(after);
};

const page = <T extends { readonly id: Id<string> }>(values: readonly T[], limit: number): Page<T, Cursor> => {
	const items = values.slice(0, limit);
	if (values.length <= limit) return { items };
	return { items, next: { after: items.at(-1)!.id } };
};

const scopeColumns = (scope: DocumentRecord["scope"]): ScopeColumns => {
	switch (scope.kind) {
		case "session":
			return { scopeKind: "session", ownerId: 0 };
		case "conversation":
			return { scopeKind: "conversation", ownerId: scope.conversationId };
		case "task":
			return { scopeKind: "task", ownerId: scope.taskId };
	}
};

const addressParts = (address: DocumentAddress | DocumentCreate | DocumentRecord) => {
	const scope = scopeColumns(address.scope);
	return {
		kind: encodeIndexedString(address.kind),
		...scope,
		family: address.key === undefined ? 0 : 1,
		keyValue: encodeIndexedString(address.key ?? ""),
	};
};

const addressKey = (address: DocumentAddress | DocumentCreate | DocumentRecord): string => {
	const parts = addressParts(address);
	return JSON.stringify([parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue]);
};

const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
	if (at === "current") return record.retiredAt === undefined;
	return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt);
};

const isCurrentOnly = (record: DocumentRecord): boolean =>
	record.scope.kind !== "conversation" || record.history === "latest";

/** Entries loaded per conversation on first use, and the most kept as commits extend the window. */
const ENTRY_WINDOW_LOAD = 256;
const ENTRY_WINDOW_MAX = 1024;

const requestKey = (conversationId: ConversationId, requestId: string): string => `${conversationId}:${requestId}`;

/** Index of the first element greater than `value` in an ascending array. */
const upperBound = (values: readonly number[], value: number): number => {
	let low = 0;
	let high = values.length;
	while (low < high) {
		const middle = (low + high) >>> 1;
		if (values[middle] <= value) low = middle + 1;
		else high = middle;
	}
	return low;
};

const writeId = (write: StorageWrite): Id<string> | undefined => {
	switch (write.type) {
		case "conversation":
		case "entry":
		case "task":
		case "submission":
			return write.value.id;
		case "document.create":
		case "document.copy":
			return write.record.id;
		case "document.change":
		case "document.retire":
			return undefined;
	}
};

const lockKey = (schema: string, session: string): string => `pi-durable:session:${schema}:${session}`;

/**
 * Postgres implementation of the Pico storage contract.
 *
 * One process owns a Session at a time. `open()` takes a session-level advisory lock on a client it keeps checked out,
 * and writes a fresh owner token that every commit checks under a row lock. If the lock connection drops, the storage
 * stops accepting work; if another process has since opened the Session, a commit from this one is rejected.
 */
export class PostgresStorage implements Storage {
	private readonly pool: PostgresPool;
	private readonly lockClient: PostgresPoolClient;
	private readonly onLockError: (error: Error) => void;
	private readonly session: string;
	private readonly lockKey: string;
	private readonly owner: string;
	private readonly tables: Tables;
	private nextId: number;
	/** The sequence the next commit takes. This storage is the session's only writer, so it advances by one per commit. */
	private nextSeq: number;
	private readonly commitFunction: string;
	private closed = false;
	private lost: Error | undefined;
	private closing: Promise<void> | undefined;
	/**
	 * Conversation records read so far. This storage is the session's only writer and conversation records are
	 * only ever inserted, so a record stays valid until a commit of this storage writes that conversation.
	 */
	private readonly conversations = new Map<ConversationId, ConversationRecord>();
	// Read-through caches with the same justification: this storage is the Session's only writer, so a record it
	// has read stays valid until it writes that record, and every write passes through `commit` below. Values are
	// kept JSON-encoded so a caller gets a detached object each time, as from the database.
	private readonly tasks = new Map<TaskId, string>();
	private readonly submissions = new Map<SubmissionId, string>();
	// A request key with no submission is remembered too: nothing else can create one, so a miss stays a miss
	// until this storage commits that submission.
	private readonly submissionsByRequest = new Map<string, SubmissionId | null>();
	private readonly entries = new Map<EntryId, { readonly record: string; readonly commitSeq: Seq }>();
	private readonly entryWindows = new Map<ConversationId, EntryWindow>();
	private readonly headMarkers = new Map<ConversationId, EntryId[]>();
	private readonly documentStates = new Map<DocumentId, DocumentState>();
	private documentScans = new Map<string, string[]>();
	private documentAddresses = new Map<string, string>();
	private admitted = 0;
	private drained: (() => void) | undefined;

	private constructor(
		pool: PostgresPool,
		lockClient: PostgresPoolClient,
		onLockError: (error: Error) => void,
		options: { session: string; schema: string; lockKey: string; owner: string; nextId: number; nextSeq: number },
	) {
		this.pool = pool;
		this.lockClient = lockClient;
		this.onLockError = onLockError;
		this.session = options.session;
		this.lockKey = options.lockKey;
		this.owner = options.owner;
		this.nextId = options.nextId;
		this.nextSeq = options.nextSeq;
		const schema = quoteIdentifier(options.schema);
		this.commitFunction = `${schema}.commit_batch`;
		this.tables = {
			sessions: `${schema}.sessions`,
			recordIds: `${schema}.record_ids`,
			conversations: `${schema}.conversations`,
			entries: `${schema}.entries`,
			tasks: `${schema}.tasks`,
			submissions: `${schema}.submissions`,
			documents: `${schema}.documents`,
			revisions: `${schema}.document_revisions`,
		};
	}

	/**
	 * Migrate the schema, lock the Session, and take ownership of it. Rejects when another process holds the Session.
	 */
	static async open(pool: PostgresPool, options: PostgresStorageOptions): Promise<PostgresStorage> {
		if (options.session.length === 0) throw new TypeError("PostgresStorage requires a non-empty session");
		const schema = options.schema ?? DEFAULT_POSTGRES_SCHEMA;
		if (options.migrate !== false) await applyPostgresMigrations(pool, schema);

		const lockClient = await pool.connect();
		let storage: PostgresStorage | undefined;
		let lockError: Error | undefined;
		const onLockError = (error: Error) => {
			lockError = error;
			storage?.loseLock(error);
		};
		lockClient.on("error", onLockError);
		const key = lockKey(schema, options.session);
		let locked = false;
		try {
			const lock = await lockClient.query<{ readonly locked: boolean }>(
				"SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked",
				[key],
			);
			if (lock.rows[0]?.locked !== true) {
				throw new Error(`Durable Postgres session ${options.session} is open in another process`);
			}
			locked = true;
			const owner = globalThis.crypto.randomUUID();
			const metadata = (
				await lockClient.query<MetadataRow>(
					`INSERT INTO ${quoteIdentifier(schema)}.sessions (session_id, next_id, next_seq, owner)
						VALUES ($1, 2, 1, $2)
						ON CONFLICT (session_id) DO UPDATE SET owner = excluded.owner
						RETURNING next_id, next_seq`,
					[options.session, owner],
				)
			).rows[0];
			if (metadata === undefined) throw new Error("Durable Postgres session metadata is missing");
			storage = new PostgresStorage(pool, lockClient, onLockError, {
				session: options.session,
				schema,
				lockKey: key,
				owner,
				nextId: Number(metadata.next_id),
				nextSeq: Number(metadata.next_seq),
			});
			if (lockError !== undefined) storage.loseLock(lockError);
			return storage;
		} catch (error) {
			let discard = lockError;
			if (locked && discard === undefined) {
				try {
					await lockClient.query("SELECT pg_advisory_unlock(hashtextextended($1, 0))", [key]);
				} catch (unlockError) {
					// Preserve the open failure; discarding the client closes its connection, which releases the lock.
					discard = unlockError as Error;
				}
			}
			lockClient.off("error", onLockError);
			lockClient.release(discard);
			throw error;
		}
	}

	commit(writes: readonly StorageWrite[], _context: Context): Promise<Seq> {
		return this.admit(async () => {
			const documentActions = this.prepareDocumentActions(writes);
			const candidateNextId = this.candidateNextId(writes);
			this.checkBatchIds(writes);
			const seq = seqFromNumber(this.nextSeq);
			const documentWrites = await this.prepareDocumentWrites(documentActions, seq);
			// One round trip: the commit function locks the session row, checks that this storage still owns it
			// at the sequence it expects, validates the batch against the stored state, and writes every table.
			try {
				await this.pool.query(`SELECT ${this.commitFunction}($1, $2, $3, $4, $5::json)`, [
					this.session,
					this.owner,
					seq,
					candidateNextId,
					this.encodeBatch(writes, documentWrites),
				]);
			} catch (error) {
				throw this.commitError(error);
			}
			this.nextSeq = seq + 1;
			this.nextId = Math.max(this.nextId, candidateNextId);
			this.absorb(writes, documentActions, documentWrites, seq);
			return seq;
		});
	}

	/** Maps an error raised by the commit function to the adapter's own errors; other errors pass through. */
	private commitError(error: unknown): unknown {
		const code = (error as { readonly code?: unknown }).code;
		const message = (error as { readonly message?: unknown }).message;
		if (typeof code !== "string" || typeof message !== "string" || !(code in commitErrorCodes)) return error;
		if (code === "DU002") {
			const lost = new Error(message);
			this.loseLock(lost);
			return lost;
		}
		return new Error(message);
	}

	/** The batch as the commit function reads it. Records and content go as JSON text, cast to `json` on insert. */
	private encodeBatch(writes: readonly StorageWrite[], documents: DocumentWrites): string {
		const claims = new Map<Id<string>, TableName>();
		const conversations: unknown[] = [];
		const entries: unknown[] = [];
		const tasks = new Map<Id<string>, unknown>();
		const submissions = new Map<Id<string>, unknown>();
		for (const write of writes) {
			switch (write.type) {
				case "conversation":
					claims.set(write.value.id, "conversation");
					conversations.push({
						id: write.value.id,
						owner_conversation_id: write.value.owner?.conversationId ?? null,
						owner_task_id: write.value.owner?.taskId ?? null,
						record: encodeJson(write.value),
					});
					break;
				case "entry":
					claims.set(write.value.id, "entry");
					entries.push({
						id: write.value.id,
						conversation_id: write.value.conversationId,
						head: write.value.head ?? null,
						record: encodeJson(write.value),
					});
					break;
				case "task":
					claims.set(write.value.id, "task");
					tasks.set(write.value.id, {
						id: write.value.id,
						conversation_id: write.value.conversationId,
						kind: encodeIndexedString(write.value.kind),
						status: write.value.state.status,
						abort_requested: write.value.abortRequested,
						background: write.value.background,
						record: encodeJson(write.value),
					});
					break;
				case "submission":
					claims.set(write.value.id, "submission");
					submissions.set(write.value.id, {
						id: write.value.id,
						conversation_id: write.value.conversationId,
						request_id: write.value.requestId === undefined ? null : encodeIndexedString(write.value.requestId),
						status: write.value.status,
						record: encodeJson(write.value),
					});
					break;
				case "document.create":
				case "document.copy":
				case "document.change":
				case "document.retire":
					break;
			}
		}
		for (const id of documents.claims) claims.set(id, "document");
		return encodeJson({
			claims: [...claims].map(([id, type]) => ({ id, type })),
			conversations,
			entries,
			tasks: [...tasks.values()],
			submissions: [...submissions.values()],
			documents: documents.rows,
		});
	}

	async mintId<I extends Id<string>>(): Promise<I> {
		this.assertOpen();
		if (!Number.isSafeInteger(this.nextId)) throw new Error("ID space is exhausted");
		return idFromNumber<I>(this.nextId++);
	}

	conversation(id: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
		return this.admit(() => this.readConversation(id));
	}

	scanConversations(
		query: ConversationQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<ConversationRecord, Cursor>> {
		return this.admit(async () => {
			const params: PostgresValue[] = [this.session];
			const clauses = ["session_id = $1", `id > $${params.push(cursorId(cursor) ?? -1)}`];
			if (query.ownerConversationId !== undefined) {
				clauses.push(`owner_conversation_id = $${params.push(query.ownerConversationId)}`);
			}
			if (query.ownerTaskId !== undefined) clauses.push(`owner_task_id = $${params.push(query.ownerTaskId)}`);
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.conversations} WHERE ${clauses.join(" AND ")}
					ORDER BY id LIMIT $${params.push(limit + 1)}`,
				params,
			);
			return page(rows.map(parseJson<ConversationRecord>), limit);
		});
	}

	entry(id: EntryId, context: Context): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	entry(
		conversationId: ConversationId,
		id: EntryId,
		context: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	entry(
		idOrConversationId: EntryId | ConversationId,
		idOrContext: EntryId | Context,
		context?: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined> {
		return this.admit(() => this.readEntry(idOrConversationId, idOrContext, context));
	}

	findLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
		_context: Context,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		return this.admit(() => this.readLatestHeadMarker(conversationId, atOrBeforeEntryId));
	}

	scanEntries(
		query: EntryQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<EntryRecord, Cursor>> {
		return this.admit(() => this.readEntries(query, limit, cursor));
	}

	private async readEntry(
		idOrConversationId: EntryId | ConversationId,
		idOrContext: EntryId | Context,
		context?: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined> {
		const id =
			context === undefined
				? idFromNumber<EntryId>(idOrConversationId)
				: typeof idOrContext === "number"
					? idFromNumber<EntryId>(idOrContext)
					: undefined;
		if (id === undefined) throw new TypeError("Storage.entry() requires an entry ID");
		let conversation: ConversationRecord | undefined;
		if (context !== undefined) {
			const conversationId = idFromNumber<ConversationId>(idOrConversationId);
			conversation = await this.readConversation(conversationId);
			if (conversation === undefined) throw new Error(`Unknown conversation: ${conversationId}`);
		}
		let stored = this.entries.get(id);
		if (stored === undefined) {
			const row = (
				await this.pool.query<EntryJsonRow>(
					`SELECT record::text AS record, commit_seq FROM ${this.tables.entries} WHERE session_id = $1 AND id = $2`,
					[this.session, id],
				)
			).rows[0];
			if (row === undefined) return undefined;
			stored = { record: row.record, commitSeq: seqFromNumber(Number(row.commit_seq)) };
			this.entries.set(id, stored);
		}
		const entry = parseJson<EntryRecord>(stored.record);
		if (conversation !== undefined) {
			let upperEntryId = Number.POSITIVE_INFINITY;
			while (conversation.id !== entry.conversationId) {
				if (conversation.parent === undefined) return undefined;
				upperEntryId = Math.min(upperEntryId, conversation.parent.at);
				conversation = (await this.readConversation(conversation.parent.conversationId))!;
			}
			if (entry.id > upperEntryId) return undefined;
		}
		return { entry, commitSeq: stored.commitSeq };
	}

	private async readLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		let conversation = await this.readConversation(conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${conversationId}`);
		let upper = atOrBeforeEntryId ?? Number.POSITIVE_INFINITY;
		while (true) {
			const ids = await this.headMarkerIds(conversation.id);
			const index = upperBound(ids, upper) - 1;
			if (index >= 0)
				return parseJson<EntryRecord & { readonly head: EntryId }>(this.entries.get(ids[index])!.record);
			if (conversation.parent === undefined) return undefined;
			upper = Math.min(upper, conversation.parent.at);
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
	}

	/** Every head marker of one conversation, ascending; loaded once, extended by commits. */
	private async headMarkerIds(conversationId: ConversationId): Promise<EntryId[]> {
		let ids = this.headMarkers.get(conversationId);
		if (ids !== undefined) return ids;
		const rows = (
			await this.pool.query<EntryJsonRow>(
				`SELECT record::text AS record, commit_seq FROM ${this.tables.entries}
					WHERE session_id = $1 AND conversation_id = $2 AND head IS NOT NULL ORDER BY id`,
				[this.session, conversationId],
			)
		).rows;
		ids = [];
		for (const row of rows) {
			const entry = parseJson<EntryRecord>(row.record);
			this.entries.set(entry.id, { record: row.record, commitSeq: seqFromNumber(Number(row.commit_seq)) });
			ids.push(entry.id);
		}
		this.headMarkers.set(conversationId, ids);
		return ids;
	}

	private async readEntries(
		query: EntryQuery,
		limit: number,
		cursor: Cursor | undefined,
	): Promise<Page<EntryRecord, Cursor>> {
		let conversation = await this.readConversation(query.conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${query.conversationId}`);
		const after = cursorId(cursor);
		let upper: number | undefined = query.maxEntryId;
		if (after !== undefined) upper = Math.min(upper ?? Number.MAX_SAFE_INTEGER, after - 1);
		const values: EntryRecord[] = [];
		while (true) {
			const rows = await this.entriesBelow(conversation.id, query.minEntryId, upper, limit + 1 - values.length);
			values.push(...rows);
			if (values.length > limit || conversation.parent === undefined) break;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			if (query.minEntryId !== undefined && upper < query.minEntryId) break;
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
		return page(values, limit);
	}

	/**
	 * Up to `limit` of one conversation's own entries with ids in `[min, upper]`, newest first. Served from the
	 * conversation's window when it covers the range; otherwise the part below the window is read, and extends
	 * the window when it is contiguous with it.
	 */
	private async entriesBelow(
		conversationId: ConversationId,
		min: number | undefined,
		upper: number | undefined,
		limit: number,
	): Promise<EntryRecord[]> {
		const window = await this.entryWindow(conversationId);
		const out: EntryRecord[] = [];
		let index = upper === undefined ? window.ids.length - 1 : upperBound(window.ids, upper) - 1;
		for (; index >= 0 && out.length < limit; index--) {
			const id = window.ids[index];
			if (min !== undefined && id < min) return out;
			out.push(parseJson<EntryRecord>(this.entries.get(id)!.record));
		}
		if (out.length >= limit || window.complete) return out;
		const oldest = window.ids[0];
		const belowWindow = upper === undefined || oldest === undefined || upper >= oldest;
		const dbUpper = belowWindow && oldest !== undefined ? oldest - 1 : upper;
		const params: PostgresValue[] = [this.session, conversationId];
		const clauses = ["session_id = $1", "conversation_id = $2"];
		if (min !== undefined) clauses.push(`id >= $${params.push(min)}`);
		if (dbUpper !== undefined) clauses.push(`id <= $${params.push(dbUpper)}`);
		const wanted = limit - out.length;
		const rows = (
			await this.pool.query<EntryJsonRow>(
				`SELECT record::text AS record, commit_seq FROM ${this.tables.entries} WHERE ${clauses.join(" AND ")}
					ORDER BY id DESC LIMIT $${params.push(wanted)}`,
				params,
			)
		).rows;
		const fetched = rows.map((row) => ({ entry: parseJson<EntryRecord>(row.record), row }));
		if (belowWindow) {
			// Contiguous with the window: keep these so a later scan of the same range needs no read. Fewer rows
			// than asked for, with no lower bound cutting them off, means the conversation starts here.
			for (const { entry, row } of fetched) {
				this.entries.set(entry.id, { record: row.record, commitSeq: seqFromNumber(Number(row.commit_seq)) });
			}
			window.ids.unshift(...fetched.map(({ entry }) => entry.id).reverse());
			if (fetched.length < wanted && min === undefined) window.complete = true;
		}
		out.push(...fetched.map(({ entry }) => entry));
		return out;
	}

	/** The conversation's entry window, loaded from its newest entries on first use. */
	private async entryWindow(conversationId: ConversationId): Promise<EntryWindow> {
		let window = this.entryWindows.get(conversationId);
		if (window !== undefined) return window;
		const rows = (
			await this.pool.query<EntryJsonRow>(
				`SELECT record::text AS record, commit_seq FROM ${this.tables.entries}
					WHERE session_id = $1 AND conversation_id = $2 ORDER BY id DESC LIMIT $3`,
				[this.session, conversationId, ENTRY_WINDOW_LOAD + 1],
			)
		).rows;
		const ids: EntryId[] = [];
		for (const row of rows.slice(0, ENTRY_WINDOW_LOAD).reverse()) {
			const entry = parseJson<EntryRecord>(row.record);
			this.entries.set(entry.id, { record: row.record, commitSeq: seqFromNumber(Number(row.commit_seq)) });
			ids.push(entry.id);
		}
		window = { ids, complete: rows.length <= ENTRY_WINDOW_LOAD };
		this.entryWindows.set(conversationId, window);
		return window;
	}

	/** Reflects a committed batch in the caches. Called only after the write statement reported success. */
	private absorb(
		writes: readonly StorageWrite[],
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		documents: DocumentWrites,
		seq: Seq,
	): void {
		for (const write of writes) {
			switch (write.type) {
				case "conversation":
					this.conversations.set(write.value.id, parseJson<ConversationRecord>(encodeJson(write.value)));
					break;
				case "task":
					this.tasks.set(write.value.id, encodeJson(write.value));
					break;
				case "submission":
					this.rememberSubmission(encodeJson(write.value));
					break;
				case "entry": {
					const record = encodeJson(write.value);
					this.entries.set(write.value.id, { record, commitSeq: seq });
					const window = this.entryWindows.get(write.value.conversationId);
					if (window !== undefined) {
						window.ids.push(write.value.id);
						if (window.ids.length > ENTRY_WINDOW_MAX) {
							const heads = new Set(this.headMarkers.get(write.value.conversationId) ?? []);
							for (const id of window.ids.splice(0, window.ids.length - ENTRY_WINDOW_MAX)) {
								if (!heads.has(id)) this.entries.delete(id);
							}
							window.complete = false;
						}
					}
					if (write.value.head !== undefined)
						this.headMarkers.get(write.value.conversationId)?.push(write.value.id);
					break;
				}
				default:
					break;
			}
		}
		if (actions.size === 0) return;
		for (const [id, { created, retired, content }] of documents.applied) {
			if (retired) {
				this.documentStates.delete(id);
				continue;
			}
			const state = this.documentStates.get(id);
			const record = created !== undefined ? encodeJson(created) : state?.record;
			if (content?.kind === "base" && record !== undefined) {
				this.documentStates.set(id, {
					record,
					version: content.version,
					value: parseJson<JsonObject>(encodeJson(content.value)),
					deltasSinceBase: 0,
				});
			} else if (content?.kind === "delta" && state !== undefined && state.version === content.version) {
				state.value = apply(state.value, parseJson<readonly Op[]>(encodeJson(content.ops))) as JsonObject;
				state.deltasSinceBase++;
			} else if (content !== undefined) {
				this.documentStates.delete(id);
			}
		}
		// Creating or retiring a document changes what an address or scope scan in its scope resolves to. A
		// retirement of a document whose scope is not known here invalidates every scope.
		for (const [id, { created, retired }] of documents.applied) {
			if (created === undefined && !retired) continue;
			const scope = created !== undefined ? scopeColumns(created.scope) : this.cachedDocumentScope(id);
			if (scope === undefined) {
				this.documentScans = new Map();
				this.documentAddresses = new Map();
				break;
			}
			const scanPrefix = `${scope.scopeKind}:${scope.ownerId}:`;
			for (const key of [...this.documentScans.keys()])
				if (key.startsWith(scanPrefix)) this.documentScans.delete(key);
			const addressMark = `,"${scope.scopeKind}",${scope.ownerId},`;
			for (const key of [...this.documentAddresses.keys()])
				if (key.includes(addressMark)) this.documentAddresses.delete(key);
		}
	}

	/** The scope of a document this storage has a record for, from its state or address caches. */
	private cachedDocumentScope(id: DocumentId): ScopeColumns | undefined {
		const state = this.documentStates.get(id);
		if (state !== undefined) return scopeColumns(parseJson<DocumentRecord>(state.record).scope);
		for (const record of this.documentAddresses.values()) {
			const parsed = parseJson<DocumentRecord>(record);
			if (parsed.id === id) return scopeColumns(parsed.scope);
		}
		return undefined;
	}

	private rememberSubmission(record: string): void {
		const submission = parseJson<SubmissionRecord>(record);
		this.submissions.set(submission.id, record);
		if (submission.requestId !== undefined) {
			this.submissionsByRequest.set(requestKey(submission.conversationId, submission.requestId), submission.id);
		}
	}

	task(id: TaskId, _context: Context): Promise<StoredTask | undefined> {
		return this.admit(async () => {
			const cached = this.tasks.get(id);
			if (cached !== undefined) return parseJson<StoredTask>(cached);
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.tasks} WHERE session_id = $1 AND id = $2`,
				[this.session, id],
			);
			if (record !== undefined) this.tasks.set(id, record);
			return record === undefined ? undefined : parseJson<StoredTask>(record);
		});
	}

	scanTasks(
		query: TaskQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<StoredTask, Cursor>> {
		return this.admit(async () => {
			const params: PostgresValue[] = [this.session];
			const clauses = ["session_id = $1", `id > $${params.push(cursorId(cursor) ?? -1)}`];
			if (query.conversationId !== undefined)
				clauses.push(`conversation_id = $${params.push(query.conversationId)}`);
			if (query.kind !== undefined) clauses.push(`kind = $${params.push(encodeIndexedString(query.kind))}`);
			if (query.status !== undefined) clauses.push(`status = $${params.push(query.status)}`);
			if (query.abortRequested !== undefined) {
				clauses.push(`abort_requested = $${params.push(query.abortRequested)}`);
			}
			if (query.background !== undefined) clauses.push(`background = $${params.push(query.background)}`);
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.tasks} WHERE ${clauses.join(" AND ")}
					ORDER BY id LIMIT $${params.push(limit + 1)}`,
				params,
			);
			return page(rows.map(parseJson<StoredTask>), limit);
		});
	}

	submission(id: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
		return this.admit(async () => {
			const cached = this.submissions.get(id);
			if (cached !== undefined) return parseJson<SubmissionRecord>(cached);
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.submissions} WHERE session_id = $1 AND id = $2`,
				[this.session, id],
			);
			if (record !== undefined) this.rememberSubmission(record);
			return record === undefined ? undefined : parseJson<SubmissionRecord>(record);
		});
	}

	scanSubmissions(
		query: SubmissionQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<SubmissionRecord, Cursor>> {
		return this.admit(async () => {
			const params: PostgresValue[] = [this.session];
			const clauses = ["session_id = $1", `id > $${params.push(cursorId(cursor) ?? -1)}`];
			if (query.conversationId !== undefined)
				clauses.push(`conversation_id = $${params.push(query.conversationId)}`);
			if (query.status !== undefined) clauses.push(`status = $${params.push(query.status)}`);
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.submissions} WHERE ${clauses.join(" AND ")}
					ORDER BY id LIMIT $${params.push(limit + 1)}`,
				params,
			);
			return page(rows.map(parseJson<SubmissionRecord>), limit);
		});
	}

	submissionByRequest(
		conversationId: ConversationId,
		requestId: string,
		_context: Context,
	): Promise<SubmissionRecord | undefined> {
		return this.admit(async () => {
			const key = requestKey(conversationId, requestId);
			const cachedId = this.submissionsByRequest.get(key);
			if (cachedId === null) return undefined;
			const cached = cachedId === undefined ? undefined : this.submissions.get(cachedId);
			if (cached !== undefined) return parseJson<SubmissionRecord>(cached);
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.submissions}
					WHERE session_id = $1 AND conversation_id = $2 AND request_id = $3`,
				[this.session, conversationId, encodeIndexedString(requestId)],
			);
			if (record !== undefined) this.rememberSubmission(record);
			else this.submissionsByRequest.set(key, null);
			return record === undefined ? undefined : parseJson<SubmissionRecord>(record);
		});
	}

	findDocument(address: DocumentAddress, at: DocumentPoint, _context: Context): Promise<DocumentRecord | undefined> {
		return this.admit(async () => {
			const key = at === "current" ? addressKey(address) : undefined;
			if (key !== undefined) {
				const cached = this.documentAddresses.get(key);
				if (cached !== undefined) return parseJson<DocumentRecord>(cached);
			}
			const parts = addressParts(address);
			const params: PostgresValue[] = [
				this.session,
				parts.kind,
				parts.scopeKind,
				parts.ownerId,
				parts.family,
				parts.keyValue,
			];
			const alive =
				at === "current"
					? "retired_at IS NULL"
					: `created_at <= $${params.push(at)} AND (retired_at IS NULL OR retired_at > $${params.length})`;
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.documents}
					WHERE session_id = $1 AND kind = $2 AND scope_kind = $3 AND owner_id = $4 AND family = $5
					AND key_value = $6 AND ${alive}
					ORDER BY created_at DESC LIMIT 1`,
				params,
			);
			if (key !== undefined && record !== undefined) this.documentAddresses.set(key, record);
			return record === undefined ? undefined : parseJson<DocumentRecord>(record);
		});
	}

	document(id: DocumentId, at: DocumentPoint, _context: Context): Promise<StoredDocument | undefined> {
		return this.admit(async () => {
			if (at === "current") {
				const state = this.documentStates.get(id);
				if (state !== undefined) {
					return {
						record: parseJson<DocumentRecord>(state.record),
						version: state.version,
						value: parseJson<JsonObject>(encodeJson(state.value)),
						deltasSinceBase: state.deltasSinceBase,
					};
				}
			}
			const stored = await this.materializeDocument(id, at);
			if (at === "current" && stored !== undefined) {
				this.documentStates.set(id, {
					record: encodeJson(stored.record),
					version: stored.version,
					value: parseJson<JsonObject>(encodeJson(stored.value)),
					deltasSinceBase: stored.deltasSinceBase,
				});
			}
			return stored;
		});
	}

	scanDocuments(
		query: DocumentQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<DocumentRecord, Cursor>> {
		return this.admit(async () => {
			const scope = scopeColumns(query.scope);
			const after = cursorId(cursor) ?? -1;
			const key = query.at === "current" ? `${scope.scopeKind}:${scope.ownerId}:${query.kind ?? ""}` : undefined;
			if (key !== undefined) {
				const cached = this.documentScans.get(key);
				if (cached !== undefined) {
					const records = cached.map(parseJson<DocumentRecord>).filter((record) => record.id > after);
					return page(records.slice(0, limit + 1), limit);
				}
			}
			const params: PostgresValue[] = [this.session, scope.scopeKind, scope.ownerId, after];
			const clauses = ["session_id = $1", "scope_kind = $2", "owner_id = $3", "id > $4"];
			if (query.kind !== undefined) clauses.push(`kind = $${params.push(encodeIndexedString(query.kind))}`);
			if (query.at === "current") {
				clauses.push("retired_at IS NULL");
			} else {
				clauses.push(
					`created_at <= $${params.push(query.at)}`,
					`(retired_at IS NULL OR retired_at > $${params.length})`,
				);
			}
			// A current scan is cached whole: it is read from its start without a page limit, and a scope's live
			// documents are few.
			const whole = key !== undefined && after === -1;
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.documents} WHERE ${clauses.join(" AND ")}
					ORDER BY id${whole ? "" : ` LIMIT $${params.push(limit + 1)}`}`,
				params,
			);
			if (whole) this.documentScans.set(key!, rows);
			return page(rows.slice(0, limit + 1).map(parseJson<DocumentRecord>), limit);
		});
	}

	/** Wait for admitted operations, release the session lock, and return its client. The pool stays open. */
	close(_context: Context): Promise<void> {
		if (this.closing === undefined) {
			this.closed = true;
			this.closing = this.release();
		}
		return this.closing;
	}

	private async release(): Promise<void> {
		if (this.admitted > 0) {
			await new Promise<void>((resolve) => {
				this.drained = resolve;
			});
		}
		let failure = this.lost;
		if (failure === undefined) {
			try {
				await this.lockClient.query("SELECT pg_advisory_unlock(hashtextextended($1, 0))", [this.lockKey]);
			} catch (error) {
				failure = error as Error;
			}
		}
		this.lockClient.off("error", this.onLockError);
		// Discarding a client whose unlock failed closes its connection, which releases the lock.
		this.lockClient.release(failure);
	}

	/**
	 * Run one storage operation. Close waits for admitted operations, so the session lock outlives every query and
	 * commit that started while the storage was open.
	 */
	private async admit<T>(operation: () => Promise<T>): Promise<T> {
		this.assertOpen();
		this.admitted++;
		try {
			return await operation();
		} finally {
			if (--this.admitted === 0) this.drained?.();
		}
	}

	private loseLock(error: Error): void {
		this.lost ??= error;
	}

	private async records(sql: string, params: PostgresValue[], executor: PostgresQueryable = this.pool) {
		return (await executor.query<JsonRow>(sql, params)).rows.map((row) => row.record);
	}

	private async recordById<T>(table: string, id: Id<string>, executor?: PostgresQueryable): Promise<T | undefined> {
		const [record] = await this.records(
			`SELECT record::text AS record FROM ${table} WHERE session_id = $1 AND id = $2`,
			[this.session, id],
			executor,
		);
		return record === undefined ? undefined : parseJson<T>(record);
	}

	private async readConversation(id: ConversationId): Promise<ConversationRecord | undefined> {
		const cached = this.conversations.get(id);
		if (cached !== undefined) return cached;
		const record = await this.recordById<ConversationRecord>(this.tables.conversations, id);
		if (record !== undefined) this.conversations.set(id, record);
		return record;
	}

	/** One statement reads the record and the revisions from the newest base at or before `at`. */
	private async materializeDocument(id: DocumentId, at: DocumentPoint): Promise<StoredDocument | undefined> {
		const upper = at === "current" ? Number.MAX_SAFE_INTEGER : at;
		const row = (
			await this.pool.query<{ readonly record: string | null; readonly revisions: string | null }>(
				`SELECT (SELECT x.record::text FROM ${this.tables.documents} x WHERE x.session_id = $1 AND x.id = $2) AS record,
					(SELECT json_agg(json_build_object('seq', r.seq, 'kind', r.kind, 'version', r.version, 'content', r.content)
						ORDER BY r.seq)::text
					 FROM ${this.tables.revisions} r WHERE r.session_id = $1 AND r.document_id = $2 AND r.seq <= $3
						AND r.seq >= (SELECT b.seq FROM ${this.tables.revisions} b WHERE b.session_id = $1 AND b.document_id = $2
							AND b.kind = 'base' AND b.seq <= $3 ORDER BY b.seq DESC LIMIT 1)) AS revisions`,
				[this.session, id, upper],
			)
		).rows[0];
		if (row?.record === null || row?.record === undefined) return undefined;
		const record = parseJson<DocumentRecord>(row.record);
		if (at !== "current" && isCurrentOnly(record)) {
			throw new Error(`Document ${id} does not retain historical content`);
		}
		if (!isAliveAt(record, at)) return undefined;
		const revisions =
			row.revisions === null
				? []
				: parseJson<
						readonly {
							readonly seq: number;
							readonly kind: DocumentContent["kind"];
							readonly version: number;
							readonly content: JsonValue;
						}[]
					>(row.revisions);
		const base = revisions[0];
		if (base === undefined || base.kind !== "base") throw new Error(`Document ${id} is missing a required base`);
		let value = base.content as JsonObject;
		const tail = revisions.slice(1);
		for (const revision of tail) {
			if (revision.kind !== "delta" || revision.version !== base.version) {
				throw new Error(`Document ${id} crosses a stored version boundary without a base`);
			}
			value = apply(value, revision.content as unknown as readonly Op[]) as JsonObject;
		}
		return { record, version: base.version, value, deltasSinceBase: tail.length };
	}

	private candidateNextId(writes: readonly StorageWrite[]): number {
		let nextId = this.nextId;
		for (const write of writes) {
			const id = writeId(write);
			if (id !== undefined) nextId = Math.max(nextId, id + 1);
		}
		return nextId;
	}

	/** The batch-internal id rules; whether an id already belongs to a stored record is the commit function's check. */
	private checkBatchIds(writes: readonly StorageWrite[]): void {
		const claimed = new Map<Id<string>, TableName>();
		for (const write of writes) {
			if (write.type === "document.change" || write.type === "document.retire") continue;
			const document = write.type === "document.create" || write.type === "document.copy";
			const table: TableName = document ? "document" : write.type;
			const id = document ? write.record.id : write.value.id;
			const earlier = claimed.get(id);
			if (table === "conversation" || table === "entry" || table === "document") {
				if (earlier !== undefined) throw new Error(`ID ${id} is written more than once`);
			} else if (earlier !== undefined && earlier !== table) {
				throw new Error(`ID ${id} is written as two record types`);
			}
			claimed.set(id, table);
		}
	}

	private prepareDocumentActions(writes: readonly StorageWrite[]): Map<DocumentId, DocumentAction> {
		const actions = new Map<DocumentId, DocumentAction>();
		for (const write of writes) {
			if (
				write.type !== "document.create" &&
				write.type !== "document.copy" &&
				write.type !== "document.change" &&
				write.type !== "document.retire"
			) {
				continue;
			}
			const id = write.type === "document.create" || write.type === "document.copy" ? write.record.id : write.id;
			let action = actions.get(id);
			if (action === undefined) {
				action = { retire: false };
				actions.set(id, action);
			}
			switch (write.type) {
				case "document.create":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.create = write.record;
					action.content = write.content;
					break;
				case "document.copy":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.create = write.record;
					action.copy = write.source;
					break;
				case "document.change":
					if (action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.content = write.content;
					break;
				case "document.retire":
					if (action.retire) throw new Error(`Document ${id} is retired more than once`);
					action.retire = true;
					break;
			}
		}
		return actions;
	}

	/**
	 * Resolves the batch's document actions into the rows the commit function takes, reading copy sources here.
	 * A created record carries its sequence; an existing document's retirement and any history clearing are the
	 * function's to apply, since it holds the stored record.
	 */
	private async prepareDocumentWrites(
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		seq: Seq,
	): Promise<DocumentWrites> {
		const writes: DocumentWrites = { claims: [], rows: [], applied: new Map() };
		for (const [id, action] of actions) {
			if (action.copy !== undefined && actions.has(action.copy.id)) {
				throw new StorageRejected(`Document copy ${id} source is changed in the copy batch`);
			}
			let content = action.content;
			if (action.copy !== undefined) {
				try {
					const stored = await this.materializeDocument(action.copy.id, action.copy.at);
					if (stored === undefined) throw new Error(`Fork source document ${action.copy.id} cannot be read`);
					const create = action.create!;
					if (
						stored.record.scope.kind !== "conversation" ||
						create.scope.kind !== "conversation" ||
						stored.record.kind !== create.kind ||
						stored.record.key !== create.key ||
						stored.record.history !== create.history ||
						stored.record.fork !== create.fork
					) {
						throw new Error(`Fork source document ${action.copy.id} does not match the copied record`);
					}
					content = { kind: "base", version: stored.version, value: stored.value };
				} catch (error) {
					if (error instanceof StorageRejected) throw error;
					throw new StorageRejected(`Document copy ${id} was rejected`, { cause: error });
				}
			}
			let created: DocumentRecord | undefined;
			let parts: ReturnType<typeof addressParts> | undefined;
			if (action.create !== undefined) {
				created = { ...action.create, createdAt: seq, ...(action.retire ? { retiredAt: seq } : {}) };
				parts = addressParts(created);
				writes.claims.push(id);
			}
			writes.rows.push({
				id,
				is_create: created !== undefined,
				retire: action.retire,
				kind: parts?.kind ?? null,
				family: parts?.family ?? null,
				key_value: parts?.keyValue ?? null,
				scope_kind: parts?.scopeKind ?? null,
				owner_id: parts?.ownerId ?? null,
				record: created === undefined ? null : encodeJson(created),
				content_kind: content?.kind ?? null,
				version: content?.version ?? null,
				content: content === undefined ? null : encodeJson(content.kind === "base" ? content.value : content.ops),
			});
			writes.applied.set(id, { created, retired: action.retire, content });
		}
		return writes;
	}

	private assertOpen(): void {
		if (this.closed) throw new Error("PostgresStorage is closed");
		if (this.lost !== undefined) {
			throw new Error(`PostgresStorage lost ownership of session ${this.session}`, { cause: this.lost });
		}
	}
}
