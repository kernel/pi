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
import {
	inTransaction,
	type PostgresPool,
	type PostgresPoolClient,
	type PostgresQueryable,
	type PostgresValue,
} from "./database.ts";
import { applyPostgresMigrations, DEFAULT_POSTGRES_SCHEMA, quoteIdentifier } from "./migrations.ts";

type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
type TableName = "conversation" | "entry" | "task" | "submission" | "document";
type JsonRow = { readonly record: string };
type EntryJsonRow = { readonly record: string; readonly commit_seq: string };
type RevisionRow = {
	readonly seq: string;
	readonly kind: DocumentContent["kind"];
	readonly version: number;
	readonly content: string;
};
type IdRow = { readonly id: string };
type MetadataRow = { readonly next_id: string; readonly next_seq: string };
/** A conversation's own entries over one contiguous id range ending at its newest entry, newest-last. */
type EntryWindow = { ids: EntryId[]; complete: boolean };
type DocumentState = { readonly record: string; readonly version: number; value: JsonObject; deltasSinceBase: number };
type CommitState = {
	readonly session:
		| { readonly next_id: number | string; readonly next_seq: number | string; readonly owner: string }
		| undefined;
	readonly ids: ReadonlyMap<Id<string>, TableName>;
	readonly documents: ReadonlyMap<DocumentId, DocumentRecord>;
	readonly versions: ReadonlyMap<DocumentId, number>;
};
type DocumentWrites = {
	readonly claims: DocumentId[];
	readonly inserts: PostgresValue[][];
	readonly retirements: PostgresValue[][];
	readonly clearRevisions: DocumentId[];
	readonly revisions: PostgresValue[][];
	/** The record as written and the content applied, per document, for the caches. */
	readonly applied: Map<
		DocumentId,
		{ readonly record: DocumentRecord; readonly content: DocumentContent | undefined }
	>;
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

// Column types of each table's insert list, for casting VALUES parameters.
const RECORD_ID_TYPES = ["text", "bigint", "text"];
const CONVERSATION_TYPES = ["text", "bigint", "bigint", "bigint", "json"];
const ENTRY_TYPES = ["text", "bigint", "bigint", "bigint", "bigint", "json"];
const TASK_TYPES = ["text", "bigint", "bigint", "text", "text", "boolean", "boolean", "json"];
const SUBMISSION_TYPES = ["text", "bigint", "bigint", "text", "text", "json"];
const DOCUMENT_TYPES = ["text", "bigint", "text", "smallint", "text", "text", "bigint", "bigint", "bigint", "json"];
const REVISION_TYPES = ["text", "bigint", "bigint", "text", "integer", "json"];

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

/** A Postgres array literal of record ids, for `= ANY($n::bigint[])` with the adapter's scalar parameter type. */
const arrayLiteral = (ids: readonly number[]): string => `{${[...new Set(ids)].join(",")}}`;

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
	private readonly submissionsByRequest = new Map<string, SubmissionId>();
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
		options: { session: string; schema: string; lockKey: string; owner: string; nextId: number },
	) {
		this.pool = pool;
		this.lockClient = lockClient;
		this.onLockError = onLockError;
		this.session = options.session;
		this.lockKey = options.lockKey;
		this.owner = options.owner;
		this.nextId = options.nextId;
		const schema = quoteIdentifier(options.schema);
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
			// Two round trips: one read of everything the checks need, then one statement that writes the whole
			// batch. A single statement is atomic by itself, so no transaction brackets it; the write is gated on
			// this storage still owning the session at the sequence the read observed, which is what the row lock
			// used to guarantee.
			const state = await this.readCommitState(writes, documentActions);
			if (state.session === undefined) throw new Error("Durable Postgres session metadata is missing");
			if (state.session.owner !== this.owner) {
				const error = new Error(`Durable Postgres session ${this.session} was opened by another process`);
				this.loseLock(error);
				throw error;
			}
			const committedSeq = seqFromNumber(Number(state.session.next_seq));
			this.checkGlobalIds(writes, state.ids);
			await this.checkDocumentActions(this.pool, documentActions, state.documents, state.versions);
			const documentWrites = await this.prepareDocumentWrites(
				this.pool,
				documentActions,
				state.documents,
				committedSeq,
			);
			const written = await this.writeBatch(this.pool, writes, documentWrites, committedSeq, {
				nextId: Math.max(Number(state.session.next_id), candidateNextId),
				nextSeq: committedSeq + 1,
				expectedSeq: committedSeq,
			});
			if (!written) {
				const error = new Error(`Durable Postgres session ${this.session} was opened by another process`);
				this.loseLock(error);
				throw error;
			}
			this.nextId = Math.max(this.nextId, candidateNextId);
			this.absorb(writes, documentActions, documentWrites, committedSeq);
			return committedSeq;
		});
	}

	/** Everything a commit checks before writing, read in one statement: the session row, the batch's claimed ids, and its documents. */
	private async readCommitState(
		writes: readonly StorageWrite[],
		actions: ReadonlyMap<DocumentId, DocumentAction>,
	): Promise<CommitState> {
		const ids = new Set<number>();
		for (const write of writes) {
			const id = writeId(write);
			if (id !== undefined) ids.add(id);
		}
		const deltas = [...actions].filter(([, action]) => action.content?.kind === "delta").map(([id]) => id);
		const row = (
			await this.pool.query<{ readonly state: string }>(
				`SELECT json_build_object(
					'session', (SELECT row_to_json(s) FROM ${this.tables.sessions} s WHERE s.session_id = $1),
					'ids', (SELECT coalesce(json_agg(json_build_object('id', r.id, 'type', r.record_type)), '[]'::json)
						FROM ${this.tables.recordIds} r WHERE r.session_id = $1 AND r.id = ANY($2::bigint[])),
					'documents', (SELECT coalesce(json_agg(json_build_object('id', d.id, 'record', d.record)), '[]'::json)
						FROM ${this.tables.documents} d WHERE d.session_id = $1 AND d.id = ANY($3::bigint[])),
					'versions', (SELECT coalesce(json_agg(json_build_object('id', v.document_id, 'version', v.version)), '[]'::json)
						FROM (SELECT DISTINCT ON (document_id) document_id, version FROM ${this.tables.revisions}
							WHERE session_id = $1 AND document_id = ANY($4::bigint[]) ORDER BY document_id, seq DESC) v)
				)::text AS state`,
				[this.session, arrayLiteral([...ids]), arrayLiteral([...actions.keys()]), arrayLiteral(deltas)],
			)
		).rows[0];
		const raw = parseJson<{
			readonly session: {
				readonly next_id: number | string;
				readonly next_seq: number | string;
				readonly owner: string;
			} | null;
			readonly ids: readonly { readonly id: number | string; readonly type: TableName }[];
			readonly documents: readonly { readonly id: number | string; readonly record: DocumentRecord }[];
			readonly versions: readonly { readonly id: number | string; readonly version: number }[];
		}>(row.state);
		const existingIds = new Map<Id<string>, TableName>();
		for (const entry of raw.ids) existingIds.set(idFromNumber<Id<string>>(Number(entry.id)), entry.type);
		const documents = new Map<DocumentId, DocumentRecord>();
		for (const entry of raw.documents) documents.set(idFromNumber<DocumentId>(Number(entry.id)), entry.record);
		const versions = new Map<DocumentId, number>();
		for (const entry of raw.versions) versions.set(idFromNumber<DocumentId>(Number(entry.id)), entry.version);
		return { session: raw.session ?? undefined, ids: existingIds, documents, versions };
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
		for (const [id, { record, content }] of documents.applied) {
			if (record.retiredAt !== undefined) {
				this.documentStates.delete(id);
				continue;
			}
			const state = this.documentStates.get(id);
			if (content?.kind === "base") {
				this.documentStates.set(id, {
					record: encodeJson(record),
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
		// Creating or retiring a document changes what an address or scope scan resolves to.
		if (documents.inserts.length > 0 || documents.retirements.length > 0) {
			this.documentScans = new Map();
			this.documentAddresses = new Map();
		}
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
			const cachedId = this.submissionsByRequest.get(requestKey(conversationId, requestId));
			const cached = cachedId === undefined ? undefined : this.submissions.get(cachedId);
			if (cached !== undefined) return parseJson<SubmissionRecord>(cached);
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.submissions}
					WHERE session_id = $1 AND conversation_id = $2 AND request_id = $3`,
				[this.session, conversationId, encodeIndexedString(requestId)],
			);
			if (record !== undefined) this.rememberSubmission(record);
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
			// The record and revision queries must observe one committed state; a commit between them can replace the base.
			const stored = await inTransaction(this.pool, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", (client) =>
				this.materializeDocument(client, id, at),
			);
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

	private async materializeDocument(
		executor: PostgresQueryable,
		id: DocumentId,
		at: DocumentPoint,
	): Promise<StoredDocument | undefined> {
		const record = await this.recordById<DocumentRecord>(this.tables.documents, id, executor);
		if (record === undefined) return undefined;
		if (at !== "current" && isCurrentOnly(record)) {
			throw new Error(`Document ${id} does not retain historical content`);
		}
		if (!isAliveAt(record, at)) return undefined;
		const upper = at === "current" ? Number.MAX_SAFE_INTEGER : at;
		const base = (
			await executor.query<RevisionRow>(
				`SELECT seq, kind, version, content::text AS content FROM ${this.tables.revisions}
					WHERE session_id = $1 AND document_id = $2 AND kind = 'base' AND seq <= $3
					ORDER BY seq DESC LIMIT 1`,
				[this.session, id, upper],
			)
		).rows[0];
		if (base === undefined) throw new Error(`Document ${id} is missing a required base`);
		let value = parseJson<JsonObject>(base.content);
		const tail = (
			await executor.query<RevisionRow>(
				`SELECT seq, kind, version, content::text AS content FROM ${this.tables.revisions}
					WHERE session_id = $1 AND document_id = $2 AND seq > $3 AND seq <= $4 ORDER BY seq`,
				[this.session, id, Number(base.seq), upper],
			)
		).rows;
		for (const revision of tail) {
			if (revision.kind !== "delta" || revision.version !== base.version) {
				throw new Error(`Document ${id} crosses a stored version boundary without a base`);
			}
			value = apply(value, parseJson<readonly Op[]>(revision.content)) as JsonObject;
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

	private checkGlobalIds(writes: readonly StorageWrite[], existingByID: ReadonlyMap<Id<string>, TableName>): void {
		const claimed = new Map<Id<string>, TableName>();
		for (const write of writes) {
			if (write.type === "document.change" || write.type === "document.retire") continue;
			const document = write.type === "document.create" || write.type === "document.copy";
			const table: TableName = document ? "document" : write.type;
			const id = document ? write.record.id : write.value.id;
			const existing = existingByID.get(id);
			const earlier = claimed.get(id);
			if (table === "conversation" || table === "entry" || table === "document") {
				if (existing !== undefined) throw new Error(`ID ${id} already belongs to ${existing}`);
				if (earlier !== undefined) throw new Error(`ID ${id} is written more than once`);
			} else {
				if (existing !== undefined && existing !== table)
					throw new Error(`ID ${id} already belongs to ${existing}`);
				if (earlier !== undefined && earlier !== table) throw new Error(`ID ${id} is written as two record types`);
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

	/** Validates the batch's document actions against the records read for them. */
	private async checkDocumentActions(
		executor: PostgresQueryable,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		existingByID: ReadonlyMap<DocumentId, DocumentRecord>,
		versions: ReadonlyMap<DocumentId, number>,
	): Promise<void> {
		const liveCounts = new Map<string, number>();
		for (const [id, action] of actions) {
			if (action.copy !== undefined && actions.has(action.copy.id)) {
				throw new StorageRejected(`Document copy ${id} source is changed in the copy batch`);
			}
			const existing = existingByID.get(id);
			if (action.create === undefined && existing === undefined) throw new Error(`Unknown document: ${id}`);
			if (action.create !== undefined && existing !== undefined) throw new Error(`Document ${id} already exists`);
			if (existing?.retiredAt !== undefined) throw new Error(`Document ${id} is retired`);
			if (action.content?.kind === "delta") {
				const previous = versions.get(id);
				if (previous === undefined) throw new Error(`Document ${id} delta has no base`);
				if (previous !== action.content.version) {
					throw new Error(`Document ${id} version transition requires a base`);
				}
			}
			// Only a create or retire can change how many incarnations an address has; a content change on an
			// existing document needs no lookup.
			if (action.create === undefined && !action.retire) continue;
			const record = action.create ?? existing!;
			const key = addressKey(record);
			let live = liveCounts.get(key);
			if (live === undefined) live = (await this.currentDocumentId(executor, record)) === undefined ? 0 : 1;
			if (action.retire && existing !== undefined) live--;
			if (action.create !== undefined && !action.retire) live++;
			liveCounts.set(key, live);
		}
		for (const live of liveCounts.values()) {
			if (live > 1) throw new Error("Document address already has a current incarnation");
		}
	}

	private async currentDocumentId(
		executor: PostgresQueryable,
		address: DocumentAddress | DocumentCreate | DocumentRecord,
	): Promise<DocumentId | undefined> {
		const parts = addressParts(address);
		const id = (
			await executor.query<IdRow>(
				`SELECT id FROM ${this.tables.documents}
					WHERE session_id = $1 AND kind = $2 AND scope_kind = $3 AND owner_id = $4 AND family = $5
					AND key_value = $6 AND retired_at IS NULL
					LIMIT 1`,
				[this.session, parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue],
			)
		).rows[0]?.id;
		return id === undefined ? undefined : idFromNumber<DocumentId>(Number(id));
	}

	/**
	 * Resolves the batch's document actions into rows: records to insert, retirements to apply to existing
	 * documents, revision histories to clear, and revisions to add. Copy sources are read here; the rows are
	 * written with everything else in `writeBatch`.
	 */
	private async prepareDocumentWrites(
		executor: PostgresQueryable,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		existing: ReadonlyMap<DocumentId, DocumentRecord>,
		seq: Seq,
	): Promise<DocumentWrites> {
		const writes: DocumentWrites = {
			claims: [],
			inserts: [],
			retirements: [],
			clearRevisions: [],
			revisions: [],
			applied: new Map(),
		};
		for (const [id, action] of actions) {
			let content = action.content;
			if (action.copy !== undefined) {
				try {
					const stored = await this.materializeDocument(executor, action.copy.id, action.copy.at);
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
			let record: DocumentRecord;
			if (action.create !== undefined) {
				record = { ...action.create, createdAt: seq, ...(action.retire ? { retiredAt: seq } : {}) };
				const parts = addressParts(record);
				writes.claims.push(id);
				writes.inserts.push([
					this.session,
					id,
					parts.kind,
					parts.family,
					parts.keyValue,
					parts.scopeKind,
					parts.ownerId,
					seq,
					action.retire ? seq : null,
					encodeJson(record),
				]);
			} else {
				record = existing.get(id)!;
				if (action.retire) {
					record = { ...record, retiredAt: seq };
					writes.retirements.push([this.session, id, seq, encodeJson(record)]);
				}
			}
			// A retired current-only document keeps no revisions, so one written in the same batch would only be
			// deleted again and is skipped. A base otherwise replaces a current-only document's history.
			const retiredCurrentOnly = action.retire && isCurrentOnly(record);
			if (retiredCurrentOnly || (content?.kind === "base" && isCurrentOnly(record))) writes.clearRevisions.push(id);
			if (content !== undefined && !retiredCurrentOnly) {
				const encodedContent = content.kind === "base" ? encodeJson(content.value) : encodeJson(content.ops);
				writes.revisions.push([this.session, id, seq, content.kind, content.version, encodedContent]);
			}
			writes.applied.set(id, { record, content: retiredCurrentOnly ? undefined : content });
		}
		return writes;
	}

	/**
	 * Writes the whole batch in one statement: data-modifying CTEs claim every new id, insert each table's rows,
	 * clear and add revisions, and the statement's final UPDATE advances the session. A task or submission written
	 * twice in one batch keeps its last write, as serial upserts did. Retiring an existing document is the one
	 * action written separately, since it updates rows that may also be read by the batch's other actions.
	 */
	private async writeBatch(
		executor: PostgresQueryable,
		writes: readonly StorageWrite[],
		documents: DocumentWrites,
		seq: Seq,
		session: { readonly nextId: number; readonly nextSeq: number; readonly expectedSeq: number },
	): Promise<boolean> {
		const claims = new Map<Id<string>, TableName>();
		const conversations: PostgresValue[][] = [];
		const entries: PostgresValue[][] = [];
		const tasks = new Map<Id<string>, PostgresValue[]>();
		const submissions = new Map<Id<string>, PostgresValue[]>();
		for (const write of writes) {
			switch (write.type) {
				case "conversation":
					claims.set(write.value.id, "conversation");
					conversations.push([
						this.session,
						write.value.id,
						write.value.owner?.conversationId ?? null,
						write.value.owner?.taskId ?? null,
						encodeJson(write.value),
					]);
					break;
				case "entry":
					claims.set(write.value.id, "entry");
					entries.push([
						this.session,
						write.value.id,
						write.value.conversationId,
						write.value.head ?? null,
						seq,
						encodeJson(write.value),
					]);
					break;
				case "task":
					claims.set(write.value.id, "task");
					tasks.set(write.value.id, [
						this.session,
						write.value.id,
						write.value.conversationId,
						encodeIndexedString(write.value.kind),
						write.value.state.status,
						write.value.abortRequested,
						write.value.background,
						encodeJson(write.value),
					]);
					break;
				case "submission":
					claims.set(write.value.id, "submission");
					submissions.set(write.value.id, [
						this.session,
						write.value.id,
						write.value.conversationId,
						write.value.requestId === undefined ? null : encodeIndexedString(write.value.requestId),
						write.value.status,
						encodeJson(write.value),
					]);
					break;
				case "document.create":
				case "document.copy":
				case "document.change":
				case "document.retire":
					break;
			}
		}
		for (const id of documents.claims) claims.set(id, "document");

		// Every write is gated on the guard: the session row still carries this owner at the sequence the
		// pre-read observed. When another process has taken the session over, every CTE writes nothing and the
		// final UPDATE affects no row.
		const params: PostgresValue[] = [this.session, this.owner, session.expectedSeq];
		const guard = `WHERE EXISTS (SELECT 1 FROM guard)`;
		// Parameters in a VALUES subselect carry no type inference, so each column is cast to its table type.
		const values = (rows: readonly (readonly PostgresValue[])[], casts: readonly string[]): string => {
			const names = casts.map((_, index) => `c${index}`).join(", ");
			const list = rows
				.map((row) => `(${row.map((value, index) => `$${params.push(value)}::${casts[index]}`).join(", ")})`)
				.join(", ");
			return `SELECT ${names} FROM (VALUES ${list}) AS v(${names}) ${guard}`;
		};
		const parts: string[] = [
			`guard AS (SELECT 1 FROM ${this.tables.sessions} WHERE session_id = $1 AND owner = $2 AND next_seq = $3)`,
		];
		for (const [, id, retiredAt, record] of documents.retirements) {
			parts.push(`retire_${id} AS (UPDATE ${this.tables.documents} SET retired_at = $${params.push(retiredAt)}::bigint,
				record = $${params.push(record)}::json WHERE session_id = $1 AND id = $${params.push(id)}::bigint AND EXISTS (SELECT 1 FROM guard))`);
		}
		if (claims.size > 0) {
			parts.push(`claims AS (INSERT INTO ${this.tables.recordIds} (session_id, id, record_type)
				${values(
					[...claims].map(([id, table]) => [this.session, id, table]),
					RECORD_ID_TYPES,
				)}
				ON CONFLICT (session_id, id) DO NOTHING)`);
		}
		if (conversations.length > 0) {
			parts.push(`conversations AS (INSERT INTO ${this.tables.conversations}
				(session_id, id, owner_conversation_id, owner_task_id, record) ${values(conversations, CONVERSATION_TYPES)})`);
		}
		if (entries.length > 0) {
			parts.push(`entries AS (INSERT INTO ${this.tables.entries}
				(session_id, id, conversation_id, head, commit_seq, record) ${values(entries, ENTRY_TYPES)})`);
		}
		if (tasks.size > 0) {
			parts.push(`tasks AS (INSERT INTO ${this.tables.tasks}
				(session_id, id, conversation_id, kind, status, abort_requested, background, record)
				${values([...tasks.values()], TASK_TYPES)}
				ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
				kind = excluded.kind, status = excluded.status, abort_requested = excluded.abort_requested,
				background = excluded.background, record = excluded.record)`);
		}
		if (submissions.size > 0) {
			parts.push(`submissions AS (INSERT INTO ${this.tables.submissions}
				(session_id, id, conversation_id, request_id, status, record) ${values([...submissions.values()], SUBMISSION_TYPES)}
				ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
				request_id = excluded.request_id, status = excluded.status, record = excluded.record)`);
		}
		if (documents.inserts.length > 0) {
			parts.push(`documents AS (INSERT INTO ${this.tables.documents}
				(session_id, id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
				${values(documents.inserts, DOCUMENT_TYPES)})`);
		}
		if (documents.clearRevisions.length > 0) {
			parts.push(`cleared AS (DELETE FROM ${this.tables.revisions}
				WHERE session_id = $1 AND document_id = ANY($${params.push(arrayLiteral(documents.clearRevisions))}::bigint[])
				AND EXISTS (SELECT 1 FROM guard))`);
		}
		if (documents.revisions.length > 0) {
			parts.push(`revisions AS (INSERT INTO ${this.tables.revisions}
				(session_id, document_id, seq, kind, version, content) ${values(documents.revisions, REVISION_TYPES)})`);
		}
		const update = `UPDATE ${this.tables.sessions} SET next_id = $${params.push(session.nextId)}, next_seq = $${params.push(session.nextSeq)}
			WHERE session_id = $1 AND owner = $2 AND next_seq = $3`;
		const result = await executor.query(`WITH ${parts.join(",\n")} ${update}`, params);
		return (result.rowCount ?? 0) > 0;
	}

	private assertOpen(): void {
		if (this.closed) throw new Error("PostgresStorage is closed");
		if (this.lost !== undefined) {
			throw new Error(`PostgresStorage lost ownership of session ${this.session}`, { cause: this.lost });
		}
	}
}
