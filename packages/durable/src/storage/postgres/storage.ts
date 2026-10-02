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
type RecordIdRow = { readonly record_type: TableName };
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
		await applyPostgresMigrations(pool, schema);

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
			const seq = await inTransaction(this.pool, "BEGIN", async (client) => {
				const metadata = (
					await client.query<MetadataRow & { readonly owner: string }>(
						`SELECT next_id, next_seq, owner FROM ${this.tables.sessions} WHERE session_id = $1 FOR UPDATE`,
						[this.session],
					)
				).rows[0];
				if (metadata === undefined) throw new Error("Durable Postgres session metadata is missing");
				if (metadata.owner !== this.owner) {
					const error = new Error(`Durable Postgres session ${this.session} was opened by another process`);
					this.loseLock(error);
					throw error;
				}
				const committedSeq = seqFromNumber(Number(metadata.next_seq));
				await this.checkGlobalIds(client, writes);
				await this.checkDocumentActions(client, documentActions);
				for (const write of writes) await this.applyTableWrite(client, write, committedSeq);
				await this.applyDocumentActions(client, documentActions, committedSeq);
				await client.query(`UPDATE ${this.tables.sessions} SET next_id = $2, next_seq = $3 WHERE session_id = $1`, [
					this.session,
					Math.max(Number(metadata.next_id), candidateNextId),
					committedSeq + 1,
				]);
				return committedSeq;
			});
			this.nextId = Math.max(this.nextId, candidateNextId);
			return seq;
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
		const row = (
			await this.pool.query<EntryJsonRow>(
				`SELECT record::text AS record, commit_seq FROM ${this.tables.entries} WHERE session_id = $1 AND id = $2`,
				[this.session, id],
			)
		).rows[0];
		if (row === undefined) return undefined;
		const entry = parseJson<EntryRecord>(row.record);
		if (conversation !== undefined) {
			let upperEntryId = Number.POSITIVE_INFINITY;
			while (conversation.id !== entry.conversationId) {
				if (conversation.parent === undefined) return undefined;
				upperEntryId = Math.min(upperEntryId, conversation.parent.at);
				conversation = (await this.readConversation(conversation.parent.conversationId))!;
			}
			if (entry.id > upperEntryId) return undefined;
		}
		return { entry, commitSeq: seqFromNumber(Number(row.commit_seq)) };
	}

	private async readLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		let conversation = await this.readConversation(conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${conversationId}`);
		let upper: number | undefined = atOrBeforeEntryId;
		while (true) {
			const params: PostgresValue[] = [this.session, conversation.id];
			const bound = upper === undefined ? "" : ` AND id <= $${params.push(upper)}`;
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.entries}
					WHERE session_id = $1 AND conversation_id = $2 AND head IS NOT NULL${bound}
					ORDER BY id DESC LIMIT 1`,
				params,
			);
			if (record !== undefined) return parseJson<EntryRecord & { readonly head: EntryId }>(record);
			if (conversation.parent === undefined) return undefined;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
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
			const params: PostgresValue[] = [this.session, conversation.id];
			const clauses = ["session_id = $1", "conversation_id = $2"];
			if (query.minEntryId !== undefined) clauses.push(`id >= $${params.push(query.minEntryId)}`);
			if (upper !== undefined) clauses.push(`id <= $${params.push(upper)}`);
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.entries} WHERE ${clauses.join(" AND ")}
					ORDER BY id DESC LIMIT $${params.push(limit + 1 - values.length)}`,
				params,
			);
			values.push(...rows.map(parseJson<EntryRecord>));
			if (values.length > limit || conversation.parent === undefined) break;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			if (query.minEntryId !== undefined && upper < query.minEntryId) break;
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
		return page(values, limit);
	}

	task(id: TaskId, _context: Context): Promise<StoredTask | undefined> {
		return this.admit(() => this.recordById<StoredTask>(this.tables.tasks, id));
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
		return this.admit(() => this.recordById<SubmissionRecord>(this.tables.submissions, id));
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
			const [record] = await this.records(
				`SELECT record::text AS record FROM ${this.tables.submissions}
					WHERE session_id = $1 AND conversation_id = $2 AND request_id = $3`,
				[this.session, conversationId, encodeIndexedString(requestId)],
			);
			return record === undefined ? undefined : parseJson<SubmissionRecord>(record);
		});
	}

	findDocument(address: DocumentAddress, at: DocumentPoint, _context: Context): Promise<DocumentRecord | undefined> {
		return this.admit(async () => {
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
			return record === undefined ? undefined : parseJson<DocumentRecord>(record);
		});
	}

	document(id: DocumentId, at: DocumentPoint, _context: Context): Promise<StoredDocument | undefined> {
		// The record and revision queries must observe one committed state; a commit between them can replace the base.
		return this.admit(() =>
			inTransaction(this.pool, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", (client) =>
				this.materializeDocument(client, id, at),
			),
		);
	}

	scanDocuments(
		query: DocumentQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<DocumentRecord, Cursor>> {
		return this.admit(async () => {
			const scope = scopeColumns(query.scope);
			const params: PostgresValue[] = [this.session, scope.scopeKind, scope.ownerId, cursorId(cursor) ?? -1];
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
			const rows = await this.records(
				`SELECT record::text AS record FROM ${this.tables.documents} WHERE ${clauses.join(" AND ")}
					ORDER BY id LIMIT $${params.push(limit + 1)}`,
				params,
			);
			return page(rows.map(parseJson<DocumentRecord>), limit);
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

	private readConversation(id: ConversationId): Promise<ConversationRecord | undefined> {
		return this.recordById<ConversationRecord>(this.tables.conversations, id);
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

	private async checkGlobalIds(executor: PostgresQueryable, writes: readonly StorageWrite[]): Promise<void> {
		const claimed = new Map<Id<string>, TableName>();
		for (const write of writes) {
			if (write.type === "document.change" || write.type === "document.retire") continue;
			const document = write.type === "document.create" || write.type === "document.copy";
			const table: TableName = document ? "document" : write.type;
			const id = document ? write.record.id : write.value.id;
			const existing = (
				await executor.query<RecordIdRow>(
					`SELECT record_type FROM ${this.tables.recordIds} WHERE session_id = $1 AND id = $2`,
					[this.session, id],
				)
			).rows[0]?.record_type;
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

	private async checkDocumentActions(
		executor: PostgresQueryable,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
	): Promise<void> {
		const liveCounts = new Map<string, number>();
		for (const [id, action] of actions) {
			if (action.copy !== undefined && actions.has(action.copy.id)) {
				throw new StorageRejected(`Document copy ${id} source is changed in the copy batch`);
			}
			const existing = await this.recordById<DocumentRecord>(this.tables.documents, id, executor);
			if (action.create === undefined && existing === undefined) throw new Error(`Unknown document: ${id}`);
			if (action.create !== undefined && existing !== undefined) throw new Error(`Document ${id} already exists`);
			if (existing?.retiredAt !== undefined) throw new Error(`Document ${id} is retired`);
			if (action.content?.kind === "delta") {
				const previous = (
					await executor.query<{ readonly version: number }>(
						`SELECT version FROM ${this.tables.revisions}
							WHERE session_id = $1 AND document_id = $2 ORDER BY seq DESC LIMIT 1`,
						[this.session, id],
					)
				).rows[0];
				if (previous === undefined) throw new Error(`Document ${id} delta has no base`);
				if (previous.version !== action.content.version) {
					throw new Error(`Document ${id} version transition requires a base`);
				}
			}
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

	private async applyTableWrite(executor: PostgresQueryable, write: StorageWrite, seq: Seq): Promise<void> {
		switch (write.type) {
			case "conversation":
				await this.claimId(executor, write.value.id, "conversation");
				await executor.query(
					`INSERT INTO ${this.tables.conversations}
						(session_id, id, owner_conversation_id, owner_task_id, record) VALUES ($1, $2, $3, $4, $5)`,
					[
						this.session,
						write.value.id,
						write.value.owner?.conversationId ?? null,
						write.value.owner?.taskId ?? null,
						encodeJson(write.value),
					],
				);
				break;
			case "entry":
				await this.claimId(executor, write.value.id, "entry");
				await executor.query(
					`INSERT INTO ${this.tables.entries} (session_id, id, conversation_id, head, commit_seq, record)
						VALUES ($1, $2, $3, $4, $5, $6)`,
					[
						this.session,
						write.value.id,
						write.value.conversationId,
						write.value.head ?? null,
						seq,
						encodeJson(write.value),
					],
				);
				break;
			case "task":
				await this.claimId(executor, write.value.id, "task");
				await executor.query(
					`INSERT INTO ${this.tables.tasks}
						(session_id, id, conversation_id, kind, status, abort_requested, background, record)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
						ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
						kind = excluded.kind, status = excluded.status, abort_requested = excluded.abort_requested,
						background = excluded.background, record = excluded.record`,
					[
						this.session,
						write.value.id,
						write.value.conversationId,
						encodeIndexedString(write.value.kind),
						write.value.state.status,
						write.value.abortRequested,
						write.value.background,
						encodeJson(write.value),
					],
				);
				break;
			case "submission":
				await this.claimId(executor, write.value.id, "submission");
				await executor.query(
					`INSERT INTO ${this.tables.submissions} (session_id, id, conversation_id, request_id, status, record)
						VALUES ($1, $2, $3, $4, $5, $6)
						ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
						request_id = excluded.request_id, status = excluded.status, record = excluded.record`,
					[
						this.session,
						write.value.id,
						write.value.conversationId,
						write.value.requestId === undefined ? null : encodeIndexedString(write.value.requestId),
						write.value.status,
						encodeJson(write.value),
					],
				);
				break;
			case "document.create":
			case "document.copy":
			case "document.change":
			case "document.retire":
				break;
		}
	}

	private async claimId(executor: PostgresQueryable, id: Id<string>, table: TableName): Promise<void> {
		await executor.query(
			`INSERT INTO ${this.tables.recordIds} (session_id, id, record_type) VALUES ($1, $2, $3)
				ON CONFLICT (session_id, id) DO NOTHING`,
			[this.session, id, table],
		);
	}

	private async applyDocumentActions(
		executor: PostgresQueryable,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		seq: Seq,
	): Promise<void> {
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
				record = {
					...action.create,
					createdAt: seq,
					...(action.retire ? { retiredAt: seq } : {}),
				};
				const parts = addressParts(record);
				await this.claimId(executor, id, "document");
				await executor.query(
					`INSERT INTO ${this.tables.documents}
						(session_id, id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
					[
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
					],
				);
			} else {
				record = (await this.recordById<DocumentRecord>(this.tables.documents, id, executor))!;
			}

			if (content !== undefined) {
				if (content.kind === "base" && isCurrentOnly(record)) await this.deleteRevisions(executor, id);
				const encodedContent = content.kind === "base" ? encodeJson(content.value) : encodeJson(content.ops);
				await executor.query(
					`INSERT INTO ${this.tables.revisions} (session_id, document_id, seq, kind, version, content)
						VALUES ($1, $2, $3, $4, $5, $6)`,
					[this.session, id, seq, content.kind, content.version, encodedContent],
				);
			}

			if (action.retire) {
				if (action.create === undefined) {
					record = { ...record, retiredAt: seq };
					await executor.query(
						`UPDATE ${this.tables.documents} SET retired_at = $3, record = $4 WHERE session_id = $1 AND id = $2`,
						[this.session, id, seq, encodeJson(record)],
					);
				}
				if (isCurrentOnly(record)) await this.deleteRevisions(executor, id);
			}
		}
	}

	private async deleteRevisions(executor: PostgresQueryable, id: DocumentId): Promise<void> {
		await executor.query(`DELETE FROM ${this.tables.revisions} WHERE session_id = $1 AND document_id = $2`, [
			this.session,
			id,
		]);
	}

	private assertOpen(): void {
		if (this.closed) throw new Error("PostgresStorage is closed");
		if (this.lost !== undefined) {
			throw new Error(`PostgresStorage lost ownership of session ${this.session}`, { cause: this.lost });
		}
	}
}
