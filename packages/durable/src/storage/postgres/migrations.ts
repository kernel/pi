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

// One commit is one call. The function locks the session row, checks that the caller still owns the session at
// the sequence it expects, validates the batch against the stored state, writes every table, and advances the
// session. Each rule raises the adapter's message with its own SQLSTATE so the adapter can map it back; see
// `commitErrorCodes` in storage.ts.
//
// Records and content travel inside the batch as JSON text and are cast to `json` on insert. Postgres accepts a
// lone UTF-16 surrogate escape when it stores `json` but rejects it in any function that decodes string values,
// such as `json_to_recordset` and `->>`; so nothing here decodes a record, and the current-only test matches the
// stored record's text.
const COMMIT_FUNCTION = `CREATE OR REPLACE FUNCTION {schema}.commit_batch(
	p_session TEXT, p_owner TEXT, p_seq BIGINT, p_next_id BIGINT, p_batch JSON
) RETURNS BIGINT LANGUAGE plpgsql AS $fn$
DECLARE
	v_owner TEXT;
	v_seq BIGINT;
	v_id BIGINT;
	v_type TEXT;
	v_exists BOOLEAN;
	v_retired BIGINT;
	v_version INTEGER;
	v_key TEXT;
	v_live JSONB := '{}'::jsonb;
	v_count INTEGER;
	doc RECORD;
	stored RECORD;
	live RECORD;
BEGIN
	SELECT owner, next_seq INTO v_owner, v_seq FROM {schema}.sessions WHERE session_id = p_session FOR UPDATE;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'Durable Postgres session metadata is missing' USING ERRCODE = 'DU001';
	END IF;
	IF v_owner <> p_owner OR v_seq <> p_seq THEN
		RAISE EXCEPTION 'Durable Postgres session % was opened by another process', p_session USING ERRCODE = 'DU002';
	END IF;

	SELECT c.id, r.record_type INTO v_id, v_type
		FROM json_to_recordset(p_batch->'claims') AS c(id BIGINT, type TEXT)
		JOIN {schema}.record_ids r ON r.session_id = p_session AND r.id = c.id
		WHERE c.type IN ('conversation', 'entry', 'document') OR r.record_type <> c.type
		LIMIT 1;
	IF FOUND THEN
		RAISE EXCEPTION 'ID % already belongs to %', v_id, v_type USING ERRCODE = 'DU003';
	END IF;

	FOR doc IN SELECT * FROM json_to_recordset(p_batch->'documents') AS x(
		id BIGINT, is_create BOOLEAN, retire BOOLEAN, kind TEXT, family SMALLINT, key_value TEXT,
		scope_kind TEXT, owner_id BIGINT, content_kind TEXT, version INTEGER)
	LOOP
		SELECT x.retired_at, x.kind, x.family, x.key_value, x.scope_kind, x.owner_id
			INTO stored FROM {schema}.documents x WHERE x.session_id = p_session AND x.id = doc.id;
		v_exists := FOUND;
		IF NOT doc.is_create AND NOT v_exists THEN
			RAISE EXCEPTION 'Unknown document: %', doc.id USING ERRCODE = 'DU004';
		END IF;
		IF doc.is_create AND v_exists THEN
			RAISE EXCEPTION 'Document % already exists', doc.id USING ERRCODE = 'DU005';
		END IF;
		IF v_exists AND stored.retired_at IS NOT NULL THEN
			RAISE EXCEPTION 'Document % is retired', doc.id USING ERRCODE = 'DU006';
		END IF;
		IF doc.content_kind = 'delta' THEN
			SELECT r.version INTO v_version FROM {schema}.document_revisions r
				WHERE r.session_id = p_session AND r.document_id = doc.id ORDER BY r.seq DESC LIMIT 1;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'Document % delta has no base', doc.id USING ERRCODE = 'DU007';
			END IF;
			IF v_version <> doc.version THEN
				RAISE EXCEPTION 'Document % version transition requires a base', doc.id USING ERRCODE = 'DU008';
			END IF;
		END IF;
		IF doc.is_create OR doc.retire THEN
			IF doc.is_create THEN
				v_key := concat_ws('|', doc.kind, doc.scope_kind, doc.owner_id, doc.family, doc.key_value);
			ELSE
				v_key := concat_ws('|', stored.kind, stored.scope_kind, stored.owner_id, stored.family, stored.key_value);
			END IF;
			IF NOT v_live ? v_key THEN
				IF doc.is_create THEN
					SELECT count(*) INTO v_count FROM {schema}.documents x WHERE x.session_id = p_session
						AND x.kind = doc.kind AND x.scope_kind = doc.scope_kind AND x.owner_id = doc.owner_id
						AND x.family = doc.family AND x.key_value = doc.key_value AND x.retired_at IS NULL;
				ELSE
					SELECT count(*) INTO v_count FROM {schema}.documents x WHERE x.session_id = p_session
						AND x.kind = stored.kind AND x.scope_kind = stored.scope_kind AND x.owner_id = stored.owner_id
						AND x.family = stored.family AND x.key_value = stored.key_value AND x.retired_at IS NULL;
				END IF;
				v_count := LEAST(v_count, 1);
			ELSE
				v_count := (v_live->>v_key)::integer;
			END IF;
			IF doc.retire AND v_exists THEN v_count := v_count - 1; END IF;
			IF doc.is_create AND NOT doc.retire THEN v_count := v_count + 1; END IF;
			v_live := v_live || jsonb_build_object(v_key, v_count);
		END IF;
	END LOOP;
	FOR live IN SELECT value FROM jsonb_each_text(v_live) LOOP
		IF live.value::integer > 1 THEN
			RAISE EXCEPTION 'Document address already has a current incarnation' USING ERRCODE = 'DU009';
		END IF;
	END LOOP;

	INSERT INTO {schema}.record_ids (session_id, id, record_type)
		SELECT p_session, c.id, c.type FROM json_to_recordset(p_batch->'claims') AS c(id BIGINT, type TEXT)
		ON CONFLICT (session_id, id) DO NOTHING;
	INSERT INTO {schema}.conversations (session_id, id, owner_conversation_id, owner_task_id, record)
		SELECT p_session, x.id, x.owner_conversation_id, x.owner_task_id, x.record::json
		FROM json_to_recordset(p_batch->'conversations')
			AS x(id BIGINT, owner_conversation_id BIGINT, owner_task_id BIGINT, record TEXT);
	INSERT INTO {schema}.entries (session_id, id, conversation_id, head, commit_seq, record)
		SELECT p_session, x.id, x.conversation_id, x.head, p_seq, x.record::json
		FROM json_to_recordset(p_batch->'entries') AS x(id BIGINT, conversation_id BIGINT, head BIGINT, record TEXT);
	INSERT INTO {schema}.tasks (session_id, id, conversation_id, kind, status, abort_requested, background, record)
		SELECT p_session, x.id, x.conversation_id, x.kind, x.status, x.abort_requested, x.background, x.record::json
		FROM json_to_recordset(p_batch->'tasks') AS x(id BIGINT, conversation_id BIGINT, kind TEXT, status TEXT,
			abort_requested BOOLEAN, background BOOLEAN, record TEXT)
		ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id, kind = excluded.kind,
			status = excluded.status, abort_requested = excluded.abort_requested, background = excluded.background,
			record = excluded.record;
	INSERT INTO {schema}.submissions (session_id, id, conversation_id, request_id, status, record)
		SELECT p_session, x.id, x.conversation_id, x.request_id, x.status, x.record::json
		FROM json_to_recordset(p_batch->'submissions')
			AS x(id BIGINT, conversation_id BIGINT, request_id TEXT, status TEXT, record TEXT)
		ON CONFLICT (session_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
			request_id = excluded.request_id, status = excluded.status, record = excluded.record;

	-- A retired record gains retiredAt by appending to its text, which keeps the rest of the record verbatim.
	UPDATE {schema}.documents x SET retired_at = p_seq,
		record = (left(x.record::text, length(x.record::text) - 1) || ',"retiredAt":' || p_seq || '}')::json
		FROM json_to_recordset(p_batch->'documents') AS d(id BIGINT, is_create BOOLEAN, retire BOOLEAN)
		WHERE x.session_id = p_session AND x.id = d.id AND d.retire AND NOT d.is_create;
	INSERT INTO {schema}.documents
		(session_id, id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
		SELECT p_session, d.id, d.kind, d.family, d.key_value, d.scope_kind, d.owner_id, p_seq,
			CASE WHEN d.retire THEN p_seq END, d.record::json
		FROM json_to_recordset(p_batch->'documents') AS d(id BIGINT, is_create BOOLEAN, retire BOOLEAN, kind TEXT,
			family SMALLINT, key_value TEXT, scope_kind TEXT, owner_id BIGINT, record TEXT)
		WHERE d.is_create;
	-- A current-only document keeps no history: a base replaces its revisions, and a retirement clears them. The
	-- record is matched as text; its only object values are the adapter's own, so the pair cannot appear elsewhere.
	DELETE FROM {schema}.document_revisions r
		USING json_to_recordset(p_batch->'documents') AS d(id BIGINT, retire BOOLEAN, content_kind TEXT)
		JOIN {schema}.documents x ON x.session_id = p_session AND x.id = d.id
		WHERE r.session_id = p_session AND r.document_id = d.id
			AND (x.scope_kind <> 'conversation' OR x.record::text LIKE '%"history":"latest"%')
			AND (d.retire OR d.content_kind = 'base');
	INSERT INTO {schema}.document_revisions (session_id, document_id, seq, kind, version, content)
		SELECT p_session, d.id, p_seq, d.content_kind, d.version, d.content::json
		FROM json_to_recordset(p_batch->'documents')
			AS d(id BIGINT, retire BOOLEAN, content_kind TEXT, version INTEGER, content TEXT)
		JOIN {schema}.documents x ON x.session_id = p_session AND x.id = d.id
		WHERE d.content_kind IS NOT NULL
			AND NOT (d.retire AND (x.scope_kind <> 'conversation' OR x.record::text LIKE '%"history":"latest"%'));

	UPDATE {schema}.sessions SET next_id = GREATEST(next_id, p_next_id), next_seq = p_seq + 1
		WHERE session_id = p_session;
	RETURN p_seq;
END
$fn$`;

/** Immutable, ordered schema history. Append new migrations after the initial schema ships. */
export const POSTGRES_MIGRATIONS: readonly PostgresMigration[] = [
	{ version: 1, statements: INITIAL_SCHEMA },
	{ version: 2, statements: [COMMIT_FUNCTION] },
];

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
