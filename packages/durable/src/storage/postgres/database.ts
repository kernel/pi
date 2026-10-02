/** Values the Postgres storage binds as query parameters. */
export type PostgresValue = null | boolean | number | string;

export interface PostgresQueryResult<R> {
	readonly rows: R[];
	readonly rowCount: number | null;
}

/** Anything that runs one parameterized statement, such as a pool or a checked-out client. */
export interface PostgresQueryable {
	query<R extends object>(text: string, values?: PostgresValue[]): Promise<PostgresQueryResult<R>>;
}

/** A connection checked out of the pool. `release(error)` discards it instead of returning it. */
export interface PostgresPoolClient extends PostgresQueryable {
	release(error?: Error | boolean): void;
	on(event: "error", listener: (error: Error) => void): unknown;
	off(event: "error", listener: (error: Error) => void): unknown;
}

/**
 * The connection pool `PostgresStorage` runs on. A `pg.Pool` satisfies it structurally, so this package
 * never imports a driver. The pool belongs to the caller: closing the storage does not end it.
 *
 * The storage keeps one client checked out for its whole lifetime to hold the session lock, so the pool
 * needs at least two connections.
 */
export interface PostgresPool extends PostgresQueryable {
	connect(): Promise<PostgresPoolClient>;
}

/**
 * Run `callback` in a transaction on its own pooled client. When BEGIN, COMMIT, or ROLLBACK fails, the client is
 * discarded so the pool never reuses a connection in an unknown transaction state.
 */
export async function inTransaction<T>(
	pool: PostgresPool,
	begin: string,
	callback: (client: PostgresQueryable) => Promise<T>,
): Promise<T> {
	const client = await pool.connect();
	let discard: Error | undefined;
	const control = async (sql: string) => {
		try {
			await client.query(sql);
		} catch (error) {
			discard = error as Error;
			throw error;
		}
	};
	try {
		await control(begin);
		let result: T;
		try {
			result = await callback(client);
		} catch (error) {
			try {
				await control("ROLLBACK");
			} catch (rollbackError) {
				throw new AggregateError([error, rollbackError], "Durable Postgres transaction failed to roll back");
			}
			throw error;
		}
		await control("COMMIT");
		return result;
	} finally {
		client.release(discard);
	}
}
