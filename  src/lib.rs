use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::{json, Map, Value};

pub struct Task {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub payload: String,
    pub retry_count: i64,
    pub max_retries: i64,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    task_name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,

    priority INTEGER NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',

    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,

    locked_by TEXT,
    locked_until REAL,

    error_log TEXT,

    scheduled_at REAL NOT NULL DEFAULT 0,
    created_at REAL NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS REAL)),
    updated_at REAL NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS REAL))
);

CREATE INDEX IF NOT EXISTS idx_tasks_pending
    ON tasks (status, scheduled_at, priority DESC, id);

CREATE INDEX IF NOT EXISTS idx_tasks_running
    ON tasks (status, locked_by);
"#;

pub fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

pub fn connect(path: &Path) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(err)?;

    conn.busy_timeout(Duration::from_millis(2_000))
        .map_err(err)?;

    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let _ = conn.pragma_update(None, "synchronous", "NORMAL");

    Ok(conn)
}

pub fn ensure_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(SCHEMA).map_err(err)
}

pub fn enqueue(
    conn: &Connection,
    task_name: &str,
    task_type: &str,
    payload: &str,
    priority: i64,
    max_retries: i64,
) -> Result<i64, String> {
    let now = now_f64();

    conn.execute(
        "INSERT INTO tasks
            (
                task_name,
                task_type,
                payload,
                priority,
                max_retries,
                status,
                retry_count,
                scheduled_at,
                created_at,
                updated_at
            )
         VALUES
            (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6, ?7, ?8)",
        params![
            task_name,
            task_type,
            payload,
            priority,
            max_retries,
            now,
            now,
            now
        ],
    )
    .map_err(err)?;

    Ok(conn.last_insert_rowid())
}

pub fn recover_expired(conn: &mut Connection) -> Result<usize, String> {
    let now = now_f64();

    let affected = conn
        .execute(
            "UPDATE tasks
             SET status = 'PENDING',
                 locked_by = NULL,
                 locked_until = NULL,
                 updated_at = ?1
             WHERE status IN ('ASSIGNED', 'RUNNING')
               AND locked_until < ?2",
            params![now, now],
        )
        .map_err(err)?;

    Ok(affected)
}

pub fn claim_next(
    conn: &mut Connection,
    worker_id: &str,
    lease_sec: f64,
) -> Result<Option<Task>, String> {
    let now = now_f64();

    let mut tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(err)?;

    let task = tx
        .query_row(
            "SELECT id, task_name, task_type, payload, retry_count, max_retries
             FROM tasks
             WHERE status = 'PENDING'
               AND scheduled_at <= ?1
             ORDER BY priority DESC, id ASC
             LIMIT 1",
            params![now],
            |row| {
                Ok(Task {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    kind: row.get(2)?,
                    payload: row.get(3)?,
                    retry_count: row.get(4)?,
                    max_retries: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(err)?;

    match task {
        Some(task) => {
            let lease_until = now + lease_sec;

            let affected = tx
                .execute(
                    "UPDATE tasks
                     SET status = 'RUNNING',
                         locked_by = ?1,
                         locked_until = ?2,
                         updated_at = ?3
                     WHERE id = ?4
                       AND status = 'PENDING'",
                    params![worker_id, lease_until, now, task.id],
                )
                .map_err(err)?;

            if affected == 0 {
                tx.rollback().map_err(err)?;
                return Ok(None);
            }

            tx.commit().map_err(err)?;
            Ok(Some(task))
        }

        None => Ok(None),
    }
}

pub fn heartbeat(
    conn: &mut Connection,
    task_id: i64,
    worker_id: &str,
    lease_sec: f64,
) -> Result<bool, String> {
    let now = now_f64();
    let lease_until = now + lease_sec;

    let affected = conn
        .execute(
            "UPDATE tasks
             SET locked_until = ?1,
                 updated_at = ?2
             WHERE id = ?3
               AND locked_by = ?4
               AND status = 'RUNNING'",
            params![lease_until, now, task_id, worker_id],
        )
        .map_err(err)?;

    Ok(affected > 0)
}

pub fn complete_task(
    conn: &mut Connection,
    task_id: i64,
    worker_id: &str,
) -> Result<(), String> {
    let now = now_f64();

    let affected = conn
        .execute(
            "UPDATE tasks
             SET status = 'COMPLETED',
                 locked_by = NULL,
                 locked_until = NULL,
                 updated_at = ?1
             WHERE id = ?2
               AND locked_by = ?3
               AND status = 'RUNNING'",
            params![now, task_id, worker_id],
        )
        .map_err(err)?;

    if affected == 0 {
        return Err("task not found, not owned by worker, or not RUNNING".to_string());
    }

    Ok(())
}

pub fn fail_task(
    conn: &mut Connection,
    task_id: i64,
    worker_id: &str,
    error_msg: &str,
) -> Result<(), String> {
    let now = now_f64();

    let mut tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(err)?;

    let row = tx
        .query_row(
            "SELECT retry_count, max_retries
             FROM tasks
             WHERE id = ?1
               AND locked_by = ?2
               AND status = 'RUNNING'",
            params![task_id, worker_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(err)?;

    match row {
        Some((retry_count, max_retries)) => {
            if retry_count + 1 >= max_retries {
                tx.execute(
                    "UPDATE tasks
                     SET status = 'FAILED',
                         retry_count = retry_count + 1,
                         locked_by = NULL,
                         locked_until = NULL,
                         error_log = ?1,
                         updated_at = ?2
                     WHERE id = ?3",
                    params![error_msg, now, task_id],
                )
                .map_err(err)?;
            } else {
                let capped_retry = retry_count.clamp(0, 20);
                let backoff_seconds = 2.0f64.powi(capped_retry as i32) * 2.0;
                let scheduled_next = now + backoff_seconds;

                tx.execute(
                    "UPDATE tasks
                     SET status = 'PENDING',
                         retry_count = retry_count + 1,
                         locked_by = NULL,
                         locked_until = NULL,
                         scheduled_at = ?1,
                         error_log = ?2,
                         updated_at = ?3
                     WHERE id = ?4",
                    params![scheduled_next, error_msg, now, task_id],
                )
                .map_err(err)?;
            }

            tx.commit().map_err(err)?;
            Ok(())
        }

        None => {
            tx.rollback().map_err(err)?;
            Err("task not found, not owned by worker, or not RUNNING".to_string())
        }
    }
}

pub fn get_task_status(conn: &Connection, task_id: i64) -> Result<Option<Value>, String> {
    conn.query_row(
        "SELECT
            id,
            task_name,
            task_type,
            payload,
            status,
            priority,
            retry_count,
            max_retries,
            locked_by,
            locked_until,
            error_log,
            scheduled_at,
            created_at,
            updated_at
         FROM tasks
         WHERE id = ?1",
        params![task_id],
        |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "name": row.get::<_, String>(1)?,
                "kind": row.get::<_, String>(2)?,
                "payload": row.get::<_, String>(3)?,
                "status": row.get::<_, String>(4)?,
                "priority": row.get::<_, i64>(5)?,
                "retry_count": row.get::<_, i64>(6)?,
                "max_retries": row.get::<_, i64>(7)?,
                "locked_by": row.get::<_, Option<String>>(8)?,
                "locked_until": row.get::<_, Option<f64>>(9)?,
                "error_log": row.get::<_, Option<String>>(10)?,
                "scheduled_at": row.get::<_, f64>(11)?,
                "created_at": row.get::<_, f64>(12)?,
                "updated_at": row.get::<_, f64>(13)?
            }))
        },
    )
    .optional()
    .map_err(err)
}

pub fn summary(conn: &Connection) -> Result<Value, String> {
    let mut stmt = conn
        .prepare("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        .map_err(err)?;

    let mut rows = stmt.query([]).map_err(err)?;
    let mut map = Map::new();

    while let Some(row) = rows.next().map_err(err)? {
        let status: String = row.get(0).map_err(err)?;
        let count: i64 = row.get(1).map_err(err)?;

        map.insert(status, json!(count));
    }

    Ok(Value::Object(map))
  }
