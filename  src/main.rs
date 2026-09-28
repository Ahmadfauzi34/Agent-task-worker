use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use agentq::{
    claim_next, complete_task, connect, ensure_schema, enqueue, fail_task, get_task_status,
    heartbeat, recover_expired, summary,
};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

#[derive(Parser)]
#[command(
    name = "agentq",
    version,
    about = "Local task tool for Linux agent environment"
)]
struct Cli {
    #[arg(long, env = "AGENTQ_DB", default_value = "agentq.db")]
    db: PathBuf,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize SQLite schema
    Schema,

    /// Enqueue a new task
    Enqueue {
        #[arg(long)]
        name: String,

        #[arg(long)]
        kind: String,

        #[arg(long, default_value = "{}")]
        payload: String,

        #[arg(long, default_value_t = 0)]
        priority: i64,

        #[arg(long, default_value_t = 3)]
        max_retries: i64,
    },

    /// Recover expired running leases
    Recover,

    /// Claim next pending task, run a command, then complete/fail automatically
    RunNext {
        #[arg(long)]
        worker: String,

        #[arg(long, default_value_t = 60.0)]
        lease_sec: f64,

        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },

    /// Show task status or summary
    Status {
        #[arg(long)]
        task_id: Option<i64>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match execute(cli) {
        Ok(value) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&value).unwrap_or_default()
            );

            ExitCode::SUCCESS
        }

        Err(err_msg) => {
            let exit: u8 = match err_msg.as_str() {
                "NO_TASK" => 2,
                "TASK_FAILED" => 3,
                "LEASE_LOST" => 4,
                "NOT_FOUND" => 5,
                _ => 1,
            };

            if exit == 1 {
                eprintln!("{}", json!({ "ok": false, "error": err_msg }));
            } else {
                println!("{}", json!({ "ok": false, "reason": err_msg }));
            }

            ExitCode::from(exit)
        }
    }
}

fn execute(cli: Cli) -> Result<Value, String> {
    let mut conn = connect(&cli.db)?;
    ensure_schema(&conn)?;

    match cli.command {
        Cmd::Schema => Ok(json!({ "ok": true })),

        Cmd::Enqueue {
            name,
            kind,
            payload,
            priority,
            max_retries,
        } => {
            let task_id = enqueue(&conn, &name, &kind, &payload, priority, max_retries)?;

            Ok(json!({
                "ok": true,
                "task_id": task_id
            }))
        }

        Cmd::Recover => {
            let recovered = recover_expired(&mut conn)?;

            Ok(json!({
                "ok": true,
                "recovered": recovered
            }))
        }

        Cmd::RunNext {
            worker,
            lease_sec,
            command,
        } => run_next(&cli.db, &worker, lease_sec, command),

        Cmd::Status { task_id } => match task_id {
            Some(task_id) => {
                let task = get_task_status(&conn, task_id)?;

                match task {
                    Some(value) => Ok(value),
                    None => Err("NOT_FOUND".to_string()),
                }
            }

            None => {
                let summary = summary(&conn)?;

                Ok(json!({
                    "ok": true,
                    "summary": summary
                }))
            }
        },
    }
}

fn run_next(
    db: &PathBuf,
    worker: &str,
    lease_sec: f64,
    command: Vec<String>,
) -> Result<Value, String> {
    if command.is_empty() {
        return Err("EMPTY_COMMAND".to_string());
    }

    let lease_sec = if lease_sec.is_finite() && lease_sec > 0.0 {
        lease_sec
    } else {
        60.0
    };

    let mut conn = connect(db)?;
    ensure_schema(&conn)?;

    let task = claim_next(&mut conn, worker, lease_sec)?
        .ok_or_else(|| "NO_TASK".to_string())?;

    let stop = Arc::new(AtomicBool::new(false));
    let lost = Arc::new(AtomicBool::new(false));

    let hb_stop = stop.clone();
    let hb_lost = lost.clone();
    let hb_db = db.clone();
    let hb_worker = worker.to_string();
    let hb_task_id = task.id;

    let heartbeat_handle = thread::spawn(move || {
        let interval = Duration::from_secs_f64((lease_sec / 2.0).max(1.0));
        let tick = Duration::from_millis(250);
        let mut since_last = Duration::ZERO;

        while !hb_stop.load(Ordering::Relaxed) {
            thread::sleep(tick);

            if hb_stop.load(Ordering::Relaxed) {
                break;
            }

            since_last += tick;

            if since_last >= interval {
                since_last = Duration::ZERO;

                match connect(&hb_db) {
                    Ok(mut hb_conn) => {
                        match heartbeat(&mut hb_conn, hb_task_id, &hb_worker, lease_sec) {
                            Ok(true) => {}
                            _ => {
                                hb_lost.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                    }

                    Err(_) => {
                        hb_lost.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
    });

    let mut child = match Command::new(&command[0])
        .args(&command[1..])
        .env("AGENTQ_DB", db.display().to_string())
        .env("AGENTQ_TASK_ID", task.id.to_string())
        .env("AGENTQ_TASK_NAME", &task.name)
        .env("AGENTQ_TASK_KIND", &task.kind)
        .env("AGENTQ_TASK_PAYLOAD", &task.payload)
        .env("AGENTQ_RETRY_COUNT", task.retry_count.to_string())
        .env("AGENTQ_MAX_RETRIES", task.max_retries.to_string())
        .spawn()
    {
        Ok(child) => child,

        Err(e) => {
            stop.store(true, Ordering::Relaxed);
            let _ = heartbeat_handle.join();

            let _ = fail_task(
                &mut conn,
                task.id,
                worker,
                &format!("spawn error: {e}"),
            );

            return Err("TASK_FAILED".to_string());
        }
    };

    let status = loop {
        if lost.load(Ordering::Relaxed) {
            let _ = child.kill();
            let _ = child.wait();

            stop.store(true, Ordering::Relaxed);
            let _ = heartbeat_handle.join();

            let _ = fail_task(
                &mut conn,
                task.id,
                worker,
                "lease lost or heartbeat failed",
            );

            return Err("LEASE_LOST".to_string());
        }

        match child.try_wait() {
            Ok(Some(status)) => break status,

            Ok(None) => {
                thread::sleep(Duration::from_millis(250));
            }

            Err(e) => {
                stop.store(true, Ordering::Relaxed);
                let _ = heartbeat_handle.join();

                let _ = fail_task(
                    &mut conn,
                    task.id,
                    worker,
                    &format!("wait error: {e}"),
                );

                return Err("TASK_FAILED".to_string());
            }
        }
    };

    stop.store(true, Ordering::Relaxed);
    let _ = heartbeat_handle.join();

    if status.success() {
        complete_task(&mut conn, task.id, worker)?;

        Ok(json!({
            "ok": true,
            "task_id": task.id,
            "exit_code": status.code()
        }))
    } else {
        let _ = fail_task(
            &mut conn,
            task.id,
            worker,
            &format!("exit code: {:?}", status.code()),
        );

        Err("TASK_FAILED".to_string())
    }
      }
