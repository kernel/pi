import { inTransaction, type PostgresPool, type PostgresQueryable } from "./database.ts";

export type PostgresMigration = {
	readonly version: number;
	/** Statements with `{schema}` standing for the quoted schema name. */
	readonly statements: readonly string[];
};

// Every table is keyed by session_id first, so one schema holds any number of independent Sessions.
// Records are `json`, not `jsonb`: `json` keeps the exact text, including key order and `\u0000` escapes that `jsonb`
// rejects.
const INITIAL_SCHEMA: readonly string[] = [
	`CREATE TABLE {schema}.sessions (
		session_id TEXT PRIMARY KEY,
		next_id BIGINT NOT NULL,
		next_seq BIGINT NOT NULL,
		owner TEXT NOT NULL
	)`,
	`CREATE TABLE {schema}.record_ids (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		record_type TEXT NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document')),
		PRIMARY KEY (session_id, id)
	)`,
	`CREATE TABLE {schema}.conversations (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		owner_conversation_id BIGINT,
		owner_task_id BIGINT,
		record JSON NOT NULL,
		PRIMARY KEY (session_id, id)
	)`,
	"CREATE INDEX conversations_by_owner_conversation ON {schema}.conversations (session_id, owner_conversation_id, id)",
	"CREATE INDEX conversations_by_owner_task ON {schema}.conversations (session_id, owner_task_id, id)",
	`CREATE TABLE {schema}.entries (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		head BIGINT,
		commit_seq BIGINT NOT NULL,
		record JSON NOT NULL,
		PRIMARY KEY (session_id, id)
	)`,
	"CREATE INDEX entries_by_conversation ON {schema}.entries (session_id, conversation_id, id DESC)",
	`CREATE INDEX entry_heads_by_conversation ON {schema}.entries (session_id, conversation_id, id DESC)
		WHERE head IS NOT NULL`,
	`CREATE TABLE {schema}.tasks (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		kind TEXT NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
		abort_requested BOOLEAN NOT NULL,
		background BOOLEAN NOT NULL,
		record JSON NOT NULL,
		PRIMARY KEY (session_id, id)
	)`,
	"CREATE INDEX tasks_by_status ON {schema}.tasks (session_id, status, id)",
	"CREATE INDEX tasks_by_conversation ON {schema}.tasks (session_id, conversation_id, id)",
	"CREATE INDEX tasks_by_kind ON {schema}.tasks (session_id, kind, id)",
	"CREATE INDEX tasks_by_abort_requested ON {schema}.tasks (session_id, abort_requested, id)",
	"CREATE INDEX tasks_by_background ON {schema}.tasks (session_id, background, id)",
	`CREATE TABLE {schema}.submissions (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		request_id TEXT,
		status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
		record JSON NOT NULL,
		PRIMARY KEY (session_id, id)
	)`,
	"CREATE INDEX submissions_by_request ON {schema}.submissions (session_id, conversation_id, request_id)",
	"CREATE INDEX submissions_by_conversation ON {schema}.submissions (session_id, conversation_id, id)",
	"CREATE INDEX submissions_by_status ON {schema}.submissions (session_id, status, id)",
	`CREATE TABLE {schema}.documents (
		session_id TEXT NOT NULL,
		id BIGINT NOT NULL,
		kind TEXT NOT NULL,
		family SMALLINT NOT NULL CHECK (family IN (0, 1)),
		key_value TEXT NOT NULL,
		scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
		owner_id BIGINT NOT NULL,
		created_at BIGINT NOT NULL,
		retired_at BIGINT,
		record JSON NOT NULL,
		PRIMARY KEY (session_id, id)
	)`,
	`CREATE INDEX documents_by_address ON {schema}.documents
		(session_id, kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at)`,
	"CREATE INDEX documents_by_scope ON {schema}.documents (session_id, scope_kind, owner_id, id)",
	"CREATE INDEX documents_by_scope_kind ON {schema}.documents (session_id, scope_kind, owner_id, kind, id)",
	`CREATE TABLE {schema}.document_revisions (
		session_id TEXT NOT NULL,
		document_id BIGINT NOT NULL,
		seq BIGINT NOT NULL,
		kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
		version INTEGER NOT NULL,
		content JSON NOT NULL,
		PRIMARY KEY (session_id, document_id, seq)
	)`,
	"CREATE INDEX document_revisions_by_kind ON {schema}.document_revisions (session_id, document_id, kind, seq DESC)",
];

/** Immutable, ordered schema history. Append new migrations after the initial schema ships. */
export const POSTGRES_MIGRATIONS: readonly PostgresMigration[] = [{ version: 1, statements: INITIAL_SCHEMA }];

export const CURRENT_POSTGRES_SCHEMA_VERSION = POSTGRES_MIGRATIONS.at(-1)?.version ?? 0;

export const DEFAULT_POSTGRES_SCHEMA = "pi_durable";

/** Quote one identifier for interpolation into SQL text. */
export const quoteIdentifier = (name: string): string => `"${name.replaceAll('"', '""')}"`;

type SchemaRow = { readonly version: number };

/**
 * Create the schema if needed and apply all pending migrations in one transaction. Concurrent callers on the
 * same schema serialize on a transaction-scoped advisory lock.
 */
export async function applyPostgresMigrations(
	pool: PostgresPool,
	schema: string = DEFAULT_POSTGRES_SCHEMA,
	migrations: readonly PostgresMigration[] = POSTGRES_MIGRATIONS,
): Promise<void> {
	for (let index = 0; index < migrations.length; index++) {
		if (migrations[index]?.version !== index + 1) {
			throw new Error("Durable Postgres migrations must have contiguous versions starting at 1");
		}
	}
	const quoted = quoteIdentifier(schema);
	await inTransaction(pool, "BEGIN", (client) => migrate(client, schema, quoted, migrations));
}

async function migrate(
	client: PostgresQueryable,
	schema: string,
	quoted: string,
	migrations: readonly PostgresMigration[],
): Promise<void> {
	await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [`pi-durable:migrate:${schema}`]);
	await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoted}`);
	await client.query(`CREATE TABLE IF NOT EXISTS ${quoted}.durable_schema (
		singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		version INTEGER NOT NULL CHECK (version >= 0)
	)`);
	await client.query(
		`INSERT INTO ${quoted}.durable_schema (singleton, version) VALUES (1, 0) ON CONFLICT (singleton) DO NOTHING`,
	);
	const row = (await client.query<SchemaRow>(`SELECT version FROM ${quoted}.durable_schema WHERE singleton = 1`))
		.rows[0];
	if (row === undefined) throw new Error("Durable Postgres schema metadata is missing");
	const currentVersion = migrations.at(-1)?.version ?? 0;
	if (row.version > currentVersion) {
		throw new Error(
			`Durable Postgres schema version ${row.version} is newer than supported version ${currentVersion}`,
		);
	}
	for (const migration of migrations) {
		if (migration.version <= row.version) continue;
		for (const statement of migration.statements) await client.query(statement.replaceAll("{schema}", quoted));
		await client.query(`UPDATE ${quoted}.durable_schema SET version = $1 WHERE singleton = 1`, [migration.version]);
	}
}
