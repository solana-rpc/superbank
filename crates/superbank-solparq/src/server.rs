use std::{
    collections::HashSet,
    future::Future,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse},
    routing::get,
};
use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::{JoinHandle, JoinSet},
};
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};

use crate::{
    archive,
    clickhouse::{DiskUsage, TableSize},
    config::{ArchiveLocation, Config},
    metrics::{AppState, ArchiveEvent, KnownDataGap, PublicStatus},
};

type ArchiveRunFuture = Pin<Box<dyn Future<Output = Result<archive::ArchiveRunReport>> + Send>>;
type ArchiveRunFactory =
    Arc<dyn Fn(archive::ArchiveKind) -> ArchiveRunFuture + Send + Sync + 'static>;
type ArchiveTaskOutput = (archive::ArchiveKind, Result<archive::ArchiveRunReport>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownSignal {
    CtrlC,
    SigTerm,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownSignal::CtrlC => formatter.write_str("ctrl-c"),
            ShutdownSignal::SigTerm => formatter.write_str("sigterm"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownWaitOutcome {
    ArchiveFinished,
    Forced,
}

#[derive(Clone)]
struct ShutdownToken {
    inner: Arc<ShutdownState>,
}

struct ShutdownState {
    requested: AtomicBool,
    notify: Notify,
}

impl ShutdownToken {
    fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownState {
                requested: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    fn request(&self) {
        if !self.inner.requested.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    fn is_requested(&self) -> bool {
        self.inner.requested.load(Ordering::SeqCst)
    }

    async fn cancelled(&self) {
        loop {
            if self.is_requested() {
                return;
            }
            let notified = self.inner.notify.notified();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

pub async fn run(config: Config) -> Result<()> {
    let state = AppState::new();
    state.set_check_interval_secs(config.archive_check_interval_secs);
    let mut servers = JoinSet::new();
    let shutdown = ShutdownToken::new();

    servers.spawn(serve_ops(config.ops_port, state.clone(), config.clone()));
    servers.spawn(serve_metrics(config.metrics_port, state.clone()));

    info!(
        ops_port = config.ops_port,
        metrics_port = config.metrics_port,
        "Solparq server mode started"
    );

    let loop_state = state.clone();
    let loop_config = config.clone();
    let loop_shutdown = shutdown.clone();
    let mut archive_task =
        tokio::spawn(async move { archive_loop(loop_config, loop_state, loop_shutdown).await });

    tokio::select! {
        signal = wait_for_shutdown_signal() => {
            let signal = signal?;
            info!(%signal, "shutdown signal received; finishing current archive before exit");
            shutdown.request();
        }
        result = servers.join_next() => {
            if let Some(result) = result {
                match result {
                    Ok(Ok(())) => {
                        info!("server task stopped; shutting down archive loop");
                        shutdown.request();
                    }
                    Ok(Err(err)) => {
                        archive_task.abort();
                        servers.abort_all();
                        return Err(err);
                    }
                    Err(err) => {
                        archive_task.abort();
                        servers.abort_all();
                        return Err(anyhow!("server task failed: {err}"));
                    }
                }
            } else {
                shutdown.request();
            }
        }
        result = &mut archive_task => {
            servers.abort_all();
            result??;
            return Ok(());
        }
    }

    let outcome =
        match wait_for_archive_task_or_forced_signal(&mut archive_task, wait_for_shutdown_signal())
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                servers.abort_all();
                return Err(err);
            }
        };
    if outcome == ShutdownWaitOutcome::ArchiveFinished {
        info!("graceful shutdown completed");
    }
    servers.abort_all();
    Ok(())
}

async fn archive_loop(config: Config, state: Arc<AppState>, shutdown: ShutdownToken) -> Result<()> {
    let mut interval =
        tokio::time::interval(Duration::from_secs(config.archive_check_interval_secs));
    let run_config = config.clone();
    let run_once: ArchiveRunFactory = Arc::new(move |kind| {
        let config = run_config.clone();
        Box::pin(async move { archive::run_once_for_kind(&config, kind).await }) as ArchiveRunFuture
    });
    let mut active_kinds = HashSet::new();
    let mut archive_tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("archive loop shutdown requested before next check");
                break;
            }
            _ = interval.tick() => {
                if !schedule_archive_cycle(
                    &config,
                    &state,
                    &shutdown,
                    &mut active_kinds,
                    &mut archive_tasks,
                    run_once.clone(),
                )? {
                    break;
                }
            }
            result = archive_tasks.join_next(), if !archive_tasks.is_empty() => {
                handle_archive_task_join(&state, &mut active_kinds, result).await?;
            }
        }
    }

    while let Some(result) = archive_tasks.join_next().await {
        handle_archive_task_join(&state, &mut active_kinds, Some(result)).await?;
    }
    Ok(())
}

fn schedule_archive_cycle(
    config: &Config,
    state: &Arc<AppState>,
    shutdown: &ShutdownToken,
    active_kinds: &mut HashSet<archive::ArchiveKind>,
    archive_tasks: &mut JoinSet<ArchiveTaskOutput>,
    run_once: ArchiveRunFactory,
) -> Result<bool> {
    info!(
        archive_types = config.archive_kinds.len(),
        "checking for archiving tasks"
    );
    for kind in config.archive_kinds.iter().copied() {
        if shutdown.is_requested() {
            info!("archive loop shutdown requested; not starting new archive task");
            return Ok(false);
        }
        if active_kinds.contains(&kind) {
            info!(
                archive_kind = kind.to_string(),
                reason = "archive task already running",
                "archive task not started"
            );
            continue;
        }
        state.record_check_started(kind, None);
        info!(archive_kind = kind.to_string(), "checking archive task");
        active_kinds.insert(kind);
        state.set_archive_in_flight(kind, true);
        spawn_archive_task(archive_tasks, run_once.clone(), kind);
    }
    Ok(true)
}

fn spawn_archive_task(
    archive_tasks: &mut JoinSet<ArchiveTaskOutput>,
    run_once: ArchiveRunFactory,
    kind: archive::ArchiveKind,
) {
    archive_tasks.spawn(async move {
        let result = AssertUnwindSafe(run_once(kind)).catch_unwind().await;
        let result = result.unwrap_or_else(|_| Err(anyhow!("archive task panicked")));
        (kind, result)
    });
}

async fn handle_archive_task_join(
    state: &Arc<AppState>,
    active_kinds: &mut HashSet<archive::ArchiveKind>,
    result: Option<std::result::Result<ArchiveTaskOutput, tokio::task::JoinError>>,
) -> Result<()> {
    let Some(result) = result else {
        return Ok(());
    };
    let (kind, result) = result.map_err(|err| anyhow!("archive task join failed: {err}"))?;
    active_kinds.remove(&kind);
    state.set_archive_in_flight(kind, false);
    record_archive_task_result(state, kind, result);
    Ok(())
}

fn record_archive_task_result(
    state: &Arc<AppState>,
    kind: archive::ArchiveKind,
    result: Result<archive::ArchiveRunReport>,
) {
    match result {
        Ok(report) => {
            if report.archive_created {
                info!(
                    archive_kind = kind.to_string(),
                    archive_name = report.archive_name.as_deref().unwrap_or("unknown"),
                    start_slot = report.archive_slot_start,
                    end_slot = report.archive_slot_end,
                    destination = report.destination,
                    "archive task created archive"
                );
            } else {
                info!(
                    archive_kind = kind.to_string(),
                    reason = report
                        .archive_skipped_reason
                        .as_deref()
                        .unwrap_or("no reason reported"),
                    "archive task skipped"
                );
            }
            debug!(
                archive_kind = kind.to_string(),
                report = report.to_text(),
                "archive task report"
            );
            state.record_report(report);
        }
        Err(err) => {
            error!(archive_kind = kind.to_string(), ?err, "archive task failed");
            state.record_task_error(kind, err.to_string());
        }
    }
}

async fn wait_for_archive_task_or_forced_signal(
    archive_task: &mut JoinHandle<Result<()>>,
    forced_shutdown: impl Future<Output = Result<ShutdownSignal>>,
) -> Result<ShutdownWaitOutcome> {
    tokio::pin!(forced_shutdown);
    tokio::select! {
        result = &mut *archive_task => {
            result??;
            Ok(ShutdownWaitOutcome::ArchiveFinished)
        }
        signal = &mut forced_shutdown => {
            let signal = signal?;
            warn!(%signal, "second shutdown signal received; aborting immediately");
            archive_task.abort();
            Ok(ShutdownWaitOutcome::Forced)
        }
    }
}

async fn wait_for_shutdown_signal() -> Result<ShutdownSignal> {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("listen for Ctrl+C")?;
                Ok(ShutdownSignal::CtrlC)
            }
            _ = sigterm.recv() => Ok(ShutdownSignal::SigTerm),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("listen for Ctrl+C")?;
        Ok(ShutdownSignal::CtrlC)
    }
}

async fn serve_ops(port: u16, state: Arc<AppState>, config: Config) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health))
        .route("/status", get(status))
        .with_state(OpsState { state, config })
        .layer(CorsLayer::permissive());
    info!("ops dashboard listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn serve_metrics(port: u16, state: Arc<AppState>) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let app = Router::new()
        .route("/metrics", get(metrics))
        .with_state(state);
    info!("metrics listening on http://{addr}/metrics");
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone)]
struct OpsState {
    state: Arc<AppState>,
    config: Config,
}

async fn dashboard(State(ops): State<OpsState>) -> Html<String> {
    Html(render_dashboard(&ops.config, &ops.state.public_status()))
}

async fn health(State(ops): State<OpsState>) -> impl IntoResponse {
    let status = ops.state.public_status();
    if status.healthy {
        (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "last_success_at_unix": status.last_success_at_unix,
                "last_error": status.last_error,
                "last_error_at_unix": status.last_error_at_unix,
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "error",
                "last_error": status.last_error,
                "last_error_at_unix": status.last_error_at_unix,
            })),
        )
    }
}

async fn status(State(ops): State<OpsState>) -> Json<serde_json::Value> {
    let status = ops.state.public_status();
    Json(json!({
        "status": status,
        "human_times": {
            "last_run_utc": format_utc_timestamp(status.last_run_at_unix),
            "last_success_utc": format_utc_timestamp(status.last_success_at_unix)
        },
        "settings": {
            "version": status.build.version,
            "git_sha": status.build.git_sha,
            "archive_kinds": ops.config.archive_kinds.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "archive_location": format!("{:?}", ops.config.archive_location),
            "output_location": ops.config.output_location,
            "transactions_table": ops.config.transactions_table,
            "blocks_table": ops.config.blocks_table,
            "gsfa_table": ops.config.gsfa_table,
            "signatures_table": ops.config.signatures_table,
            "ops_port": ops.config.ops_port,
            "metrics_port": ops.config.metrics_port,
            "archives_to_keep": ops.config.archives_to_keep,
            "continue_from_last_archive": ops.config.continue_from_last_archive,
            "delete_archived_data_range": ops.config.delete_archived_data_range,
            "force_archive": ops.config.force_archive,
            "archive_check_interval_secs": ops.config.archive_check_interval_secs
        }
    }))
}

async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        state.prometheus_text(),
    )
}

pub fn format_utc_timestamp(timestamp_unix: Option<u64>) -> String {
    let Some(timestamp_unix) = timestamp_unix else {
        return "never".to_string();
    };
    DateTime::<Utc>::from_timestamp(timestamp_unix as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "invalid timestamp".to_string())
}

pub fn render_dashboard(config: &Config, status: &PublicStatus) -> String {
    let archive_types = config
        .archive_kinds
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let (health_label, health_class) = if status.healthy {
        ("Healthy", "ok")
    } else {
        ("Needs attention", "bad")
    };
    let last_error_display = match (&status.last_error, status.last_error_at_unix) {
        (Some(err), Some(at)) => {
            format!("{err} (at {})", format_utc_timestamp(Some(at)))
        }
        (Some(err), None) => err.clone(),
        (None, _) => "none".to_string(),
    };
    let db_slots = status
        .db_slots
        .map(|slots| {
            format!(
                "{} slots ({} to {})",
                format_u64(slots.slots_available),
                format_u64(slots.earliest_slot),
                format_u64(slots.latest_slot)
            )
        })
        .unwrap_or_else(|| "unknown".to_string());
    let disk_summary = render_disk_summary(&status.disk_usage);
    let disk_rows = render_disk_usage(&status.disk_usage);
    let table_size_summary = render_table_size_summary(&status.table_sizes);
    let table_size_rows = render_table_sizes(&status.table_sizes);
    let timeline = render_timeline(&status.recent_events);
    let event_rows = render_event_rows(&status.recent_events);
    let gap_rows = render_gap_rows(&status.known_gaps);
    let archive_tables = render_archive_table_list(status);
    let output = dashboard_output(config);
    let version = format!("v{}", status.build.version);
    let version_detail = format!("{} ({})", version, status.build.git_sha);
    format!(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Solparq Ops</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f9fc;
      --panel: #ffffff;
      --ink: #18202f;
      --muted: #637083;
      --line: #d9e1ec;
      --blue: #2f6fed;
      --green: #138a5b;
      --amber: #b7791f;
      --red: #c2413f;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--bg); color: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ max-width: 1534px; margin: 0 auto; padding: 28px; }}
    header {{ display: flex; justify-content: space-between; gap: 20px; align-items: flex-start; margin-bottom: 24px; }}
    h1 {{ margin: 0 0 6px; font-size: 28px; font-weight: 720; }}
    h2 {{ margin: 0 0 14px; font-size: 17px; font-weight: 680; }}
    p {{ margin: 0; color: var(--muted); }}
    code {{ background: #eef3f8; border: 1px solid var(--line); padding: 2px 6px; border-radius: 5px; }}
    ul {{ margin: 0; padding-left: 18px; }}
    li {{ margin: 0 0 6px; }}
    li:last-child {{ margin-bottom: 0; }}
    .pill {{ display: inline-flex; align-items: center; gap: 8px; padding: 8px 12px; border-radius: 999px; font-weight: 650; background: var(--panel); border: 1px solid var(--line); }}
    .pill.ok {{ color: var(--green); }}
    .pill.bad {{ color: var(--red); }}
    .dot {{ width: 9px; height: 9px; border-radius: 50%; background: currentColor; }}
    .grid {{ display: grid; gap: 14px; grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 18px; }}
    .metric, section {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; box-shadow: 0 8px 22px rgba(24, 32, 47, .06); }}
    .metric {{ padding: 16px; min-height: 92px; }}
    .label {{ color: var(--muted); font-size: 13px; margin-bottom: 7px; }}
    .value {{ font-size: 23px; font-weight: 720; overflow-wrap: anywhere; }}
    section {{ padding: 18px; margin-bottom: 18px; }}
    .two {{ display: grid; gap: 18px; grid-template-columns: minmax(0, 1.35fr) minmax(416px, .65fr); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ width: 273px; color: var(--muted); font-weight: 620; }}
    tr:last-child th, tr:last-child td {{ border-bottom: 0; }}
    .timeline svg {{ width: 100%; height: auto; display: block; }}
    .events {{ margin-top: 12px; max-height: 260px; overflow: auto; border-top: 1px solid var(--line); }}
    .event {{ display: grid; grid-template-columns: 164px 142px 1fr; gap: 10px; padding: 10px 0; border-bottom: 1px solid var(--line); font-size: 14px; }}
    .event:last-child {{ border-bottom: 0; }}
    .badge {{ display: inline-flex; justify-content: center; border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 700; color: #fff; }}
    .gaps {{ overflow: auto; }}
    .gaps table {{ min-width: 988px; }}
    .disks {{ overflow: auto; }}
    .disks table {{ min-width: 760px; }}
    .bar {{ display: inline-block; width: 140px; height: 8px; border-radius: 4px; background: #eef3f8; border: 1px solid var(--line); vertical-align: middle; overflow: hidden; }}
    .bar-fill {{ display: block; height: 100%; background: var(--blue); }}
    .bar-fill.warn {{ background: var(--amber); }}
    .bar-fill.crit {{ background: var(--red); }}
    .created {{ background: var(--green); }}
    .skipped {{ background: var(--amber); }}
    .error {{ background: var(--red); }}
    .checking {{ background: var(--blue); }}
    @media (max-width: 860px) {{
      main {{ padding: 18px; }}
      header, .two {{ display: block; }}
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .pill {{ margin-top: 14px; }}
      .event {{ grid-template-columns: 1fr; }}
      th, td {{ display: block; width: 100%; padding-left: 0; padding-right: 0; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Solparq Ops</h1>
        <p>{version} · Auto-refreshes every 30 seconds</p>
      </div>
      <div class="pill {health_class}"><span class="dot"></span>{health_label}</div>
    </header>

    <div class="grid">
      <div class="metric"><div class="label">Slots available</div><div class="value">{db_slots}</div></div>
      <div class="metric"><div class="label">Archives created</div><div class="value">{archives_created}</div></div>
      <div class="metric"><div class="label">Last run</div><div class="value">{last_run}</div></div>
      <div class="metric"><div class="label">Last success</div><div class="value">{last_success}</div></div>
      <div class="metric"><div class="label">ClickHouse disk usage</div><div class="value">{disk_summary}</div></div>
      <div class="metric"><div class="label">DB table sizes (total)</div><div class="value">{table_size_summary}</div></div>
    </div>

    <div class="two">
      <section>
        <h2>Archive timeline</h2>
        <div class="timeline">{timeline}</div>
        <div class="events">{event_rows}</div>
      </section>
      <section>
        <h2>Startup settings</h2>
        <table>
          <tr><th>Version</th><td>{version_detail}</td></tr>
          <tr><th>Archive types</th><td>{archive_types}</td></tr>
          <tr><th>Location</th><td>{location}</td></tr>
          <tr><th>Output</th><td><code>{output}</code></td></tr>
          <tr><th>Archive tables</th><td>{archive_tables}</td></tr>
          <tr><th>Archives to keep</th><td>{archives_to_keep}</td></tr>
          <tr><th>Continue from last archive</th><td>{continue_from_last_archive}</td></tr>
          <tr><th>Check interval</th><td>{check_interval} seconds</td></tr>
          <tr><th>Skipped</th><td>{archives_skipped}</td></tr>
          <tr><th>Errors</th><td>{archive_errors}</td></tr>
          <tr><th>Last error</th><td>{last_error}</td></tr>
        </table>
      </section>
    </div>
    <section class="disks">
      <h2>ClickHouse disk usage</h2>
      {disk_rows}
    </section>
    <section class="disks">
      <h2>ClickHouse table sizes</h2>
      {table_size_rows}
    </section>
    <section class="gaps">
      <h2>Known data gaps</h2>
      {gap_rows}
    </section>
  </main>
</body>
</html>"#,
        health_class = health_class,
        health_label = health_label,
        version = html_escape(&version),
        version_detail = html_escape(&version_detail),
        db_slots = html_escape(&db_slots),
        archives_created = format_u64(status.archives_created),
        last_run = html_escape(&format_utc_timestamp(status.last_run_at_unix)),
        last_success = html_escape(&format_utc_timestamp(status.last_success_at_unix)),
        disk_summary = html_escape(&disk_summary),
        disk_rows = disk_rows,
        table_size_summary = html_escape(&table_size_summary),
        table_size_rows = table_size_rows,
        timeline = timeline,
        event_rows = event_rows,
        gap_rows = gap_rows,
        archive_types = html_escape(&archive_types),
        location = html_escape(&format!("{:?}", config.archive_location)),
        output = html_escape(&output),
        archive_tables = archive_tables,
        archives_to_keep = format_u64(config.archives_to_keep as u64),
        continue_from_last_archive = config.continue_from_last_archive,
        check_interval = format_u64(config.archive_check_interval_secs),
        archives_skipped = format_u64(status.archives_skipped),
        archive_errors = format_u64(status.archive_errors),
        last_error = html_escape(&last_error_display),
    )
}

fn render_disk_summary(disks: &[DiskUsage]) -> String {
    if disks.is_empty() {
        return "unknown".to_string();
    }
    let total: u64 = disks.iter().map(|disk| disk.total_bytes).sum();
    let used: u64 = disks.iter().map(DiskUsage::used_bytes).sum();
    let percent = usage_percent(used, total);
    format!(
        "{} / {} ({percent:.0}%)",
        format_bytes(used),
        format_bytes(total)
    )
}

fn render_disk_usage(disks: &[DiskUsage]) -> String {
    if disks.is_empty() {
        return "<p>No ClickHouse disk usage reported yet.</p>".to_string();
    }
    let rows = disks
        .iter()
        .map(|disk| {
            let used = disk.used_bytes();
            let percent = usage_percent(used, disk.total_bytes);
            let bar_class = if percent >= 90.0 {
                "crit"
            } else if percent >= 75.0 {
                "warn"
            } else {
                ""
            };
            format!(
                "<tr><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td>\
                 <td><span class=\"bar\"><span class=\"bar-fill {bar_class}\" style=\"width:{:.0}%\"></span></span> {percent:.1}%</td></tr>",
                html_escape(&disk.name),
                html_escape(&disk.path),
                format_bytes(used),
                format_bytes(disk.free_bytes),
                format_bytes(disk.total_bytes),
                percent.min(100.0),
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Disk</th><th>Path</th><th>Used</th><th>Free</th><th>Total</th><th>Usage</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn render_table_size_summary(tables: &[TableSize]) -> String {
    if tables.is_empty() {
        return "unknown".to_string();
    }
    let total_bytes: u64 = tables.iter().map(|table| table.bytes).sum();
    let total_rows: u64 = tables.iter().map(|table| table.rows).sum();
    format!(
        "{} ({} rows)",
        format_bytes(total_bytes),
        format_u64(total_rows)
    )
}

fn render_table_sizes(tables: &[TableSize]) -> String {
    if tables.is_empty() {
        return "<p>No ClickHouse table sizes reported yet.</p>".to_string();
    }
    let rows = tables
        .iter()
        .map(|table| {
            format!(
                "<tr><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                html_escape(table.kind.as_str()),
                html_escape(&table.table_name),
                format_u64(table.rows),
                format_bytes(table.bytes),
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Archive table</th><th>ClickHouse table</th><th>Rows</th><th>Size</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn usage_percent(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 / total as f64 * 100.0
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{value:.0} {}", UNITS[unit_idx])
    } else {
        format!("{value:.1} {}", UNITS[unit_idx])
    }
}

fn render_archive_table_list(status: &PublicStatus) -> String {
    let Some(report) = &status.last_report else {
        return "<p>No detected archive tables yet.</p>".to_string();
    };
    if report.archive_tables.is_empty() {
        return "<p>No detected archive tables yet.</p>".to_string();
    }
    let items = report
        .archive_tables
        .iter()
        .map(|table| format!("<li><code>{}</code></li>", html_escape(&table.table_name)))
        .collect::<String>();
    format!("<ul>{items}</ul>")
}

fn dashboard_output(config: &Config) -> String {
    match config.archive_location {
        ArchiveLocation::Local => config.output_location.display().to_string(),
        ArchiveLocation::S3 => config
            .s3
            .as_ref()
            .map(|s3| {
                let bucket_path = s3.bucket_path.trim_matches('/');
                if bucket_path.is_empty() {
                    format!("s3://{}", s3.bucket_name)
                } else {
                    format!("s3://{}/{}", s3.bucket_name, bucket_path)
                }
            })
            .unwrap_or_else(|| "s3://<missing-config>".to_string()),
    }
}

fn render_timeline(events: &[ArchiveEvent]) -> String {
    if events.is_empty() {
        return "<p>No archive events yet.</p>".to_string();
    }
    let width = 760.0;
    let height = 150.0;
    let left = 34.0;
    let right = width - 34.0;
    let span = (events.len().saturating_sub(1)).max(1) as f64;
    let points = events
        .iter()
        .enumerate()
        .map(|(idx, event)| {
            let x = left + (right - left) * (idx as f64 / span);
            let y = match event.outcome.as_str() {
                "created" => 36.0,
                "skipped" => 74.0,
                "error" => 112.0,
                _ => 94.0,
            };
            (x, y, event)
        })
        .collect::<Vec<_>>();
    let polyline = points
        .iter()
        .map(|(x, y, _)| format!("{x:.1},{y:.1}"))
        .collect::<Vec<_>>()
        .join(" ");
    let circles = points
        .iter()
        .map(|(x, y, event)| {
            format!(
                "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"6\" fill=\"{}\"><title>{} {} {}</title></circle>",
                event_color(&event.outcome),
                html_escape(&format_utc_timestamp(Some(event.timestamp_unix))),
                html_escape(&event.archive_kind),
                html_escape(event.archive_name.as_deref().or(event.reason.as_deref()).unwrap_or(&event.outcome))
            )
        })
        .collect::<String>();
    format!(
        "<svg viewBox=\"0 0 {width:.0} {height:.0}\" role=\"img\" aria-label=\"Archive event timeline\">\
         <line x1=\"34\" y1=\"36\" x2=\"726\" y2=\"36\" stroke=\"#d9e1ec\"/>\
         <line x1=\"34\" y1=\"74\" x2=\"726\" y2=\"74\" stroke=\"#d9e1ec\"/>\
         <line x1=\"34\" y1=\"112\" x2=\"726\" y2=\"112\" stroke=\"#d9e1ec\"/>\
         <text x=\"0\" y=\"40\" font-size=\"11\" fill=\"#637083\">ok</text>\
         <text x=\"0\" y=\"78\" font-size=\"11\" fill=\"#637083\">skip</text>\
         <text x=\"0\" y=\"116\" font-size=\"11\" fill=\"#637083\">err</text>\
         <polyline points=\"{polyline}\" fill=\"none\" stroke=\"#2f6fed\" stroke-width=\"2\" opacity=\"0.45\"/>\
         {circles}</svg>"
    )
}

fn render_event_rows(events: &[ArchiveEvent]) -> String {
    if events.is_empty() {
        return "<p>No recent archive events.</p>".to_string();
    }
    events
        .iter()
        .rev()
        .take(12)
        .map(|event| {
            let detail = event
                .archive_name
                .as_deref()
                .or(event.reason.as_deref())
                .unwrap_or("");
            let badge_label = match (&event.outcome, &event.skip_reason_code) {
                (outcome, Some(skip_reason_code)) if outcome == "skipped" => {
                    format!("{outcome}: {skip_reason_code}")
                }
                (outcome, _) => outcome.clone(),
            };
            format!(
                "<div class=\"event\"><div>{}</div><div><span class=\"badge {}\">{}</span></div><div><strong>{}</strong> {}</div></div>",
                html_escape(&format_utc_timestamp(Some(event.timestamp_unix))),
                html_escape(&event.outcome),
                html_escape(&badge_label),
                html_escape(&event.archive_kind),
                html_escape(detail)
            )
        })
        .collect::<String>()
}

fn render_gap_rows(gaps: &[KnownDataGap]) -> String {
    if gaps.is_empty() {
        return "<p>No known data gaps from recent archive validations.</p>".to_string();
    }
    let rows = gaps
        .iter()
        .rev()
        .take(24)
        .map(|gap| {
            let range = if gap.start_slot == gap.end_slot {
                gap.start_slot.to_string()
            } else {
                format!("{}-{}", gap.start_slot, gap.end_slot)
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(&format_utc_timestamp(Some(gap.timestamp_unix))),
                html_escape(&gap.archive_kind),
                html_escape(&gap.classification),
                html_escape(&range),
                format_u64(gap.slot_count),
                html_escape(&gap.detail)
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Seen</th><th>Archive type</th><th>Classification</th><th>Range</th><th>Slots</th><th>Detail</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn event_color(outcome: &str) -> &'static str {
    match outcome {
        "created" => "#138a5b",
        "skipped" => "#b7791f",
        "error" => "#c2413f",
        _ => "#2f6fed",
    }
}

fn format_u64(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx.is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{
        archive::{ArchiveKind, ArchiveRunReport},
        config::Config,
    };

    fn test_server_config() -> Config {
        Config::try_parse_from([
            "superbank-solparq",
            "--db-server",
            "127.0.0.1",
            "--db-user",
            "admin",
            "--db-password",
            "secret",
            "--archive-range-type",
            "hourly",
            "--archive-range-type",
            "custom:500",
            "--server-mode",
        ])
        .expect("valid config")
    }

    #[tokio::test]
    async fn shutdown_requested_before_cycle_does_not_start_new_archive_tasks() {
        let config = test_server_config();
        let state = AppState::new();
        let shutdown = ShutdownToken::new();
        shutdown.request();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_runner = seen.clone();
        let run_once: ArchiveRunFactory = Arc::new(move |kind| {
            let seen = seen_for_runner.clone();
            Box::pin(async move {
                seen.lock().expect("seen lock").push(kind);
                Ok(ArchiveRunReport::skipped(kind, "test".to_string(), "test"))
            }) as ArchiveRunFuture
        });
        let mut active_kinds = HashSet::new();
        let mut archive_tasks = JoinSet::new();

        let continue_running = schedule_archive_cycle(
            &config,
            &state,
            &shutdown,
            &mut active_kinds,
            &mut archive_tasks,
            run_once,
        )
        .expect("schedule cycle");

        assert!(!continue_running);
        assert_eq!(
            *seen.lock().expect("seen lock"),
            Vec::<ArchiveKind>::new(),
            "shutdown should not start archive tasks"
        );
    }

    #[tokio::test]
    async fn archive_cycle_starts_other_kinds_while_one_kind_is_running() {
        let config = test_server_config();
        let state = AppState::new();
        let shutdown = ShutdownToken::new();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finish_hourly_tx, finish_hourly_rx) = oneshot::channel();

        let finish_hourly_rx = Arc::new(Mutex::new(Some(finish_hourly_rx)));
        let run_once: ArchiveRunFactory = Arc::new(move |kind| {
            let started_tx = started_tx.clone();
            let finish_hourly_rx = finish_hourly_rx.clone();
            Box::pin(async move {
                started_tx.send(kind).expect("record started archive");
                let finish_hourly_rx = {
                    if kind == ArchiveKind::Hourly {
                        finish_hourly_rx.lock().expect("finish lock").take()
                    } else {
                        None
                    }
                };
                if let Some(finish_hourly_rx) = finish_hourly_rx {
                    finish_hourly_rx.await.expect("finish hourly");
                }
                Ok(ArchiveRunReport::skipped(kind, "test".to_string(), "test"))
            }) as ArchiveRunFuture
        });
        let mut active_kinds = HashSet::new();
        let mut archive_tasks = JoinSet::new();

        schedule_archive_cycle(
            &config,
            &state,
            &shutdown,
            &mut active_kinds,
            &mut archive_tasks,
            run_once,
        )
        .expect("schedule cycle");

        assert_eq!(started_rx.recv().await, Some(ArchiveKind::Hourly));
        let next_started = tokio::time::timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .expect("custom archive should start while hourly is still running");
        assert_eq!(next_started, Some(ArchiveKind::Custom { slots: 500 }));

        finish_hourly_tx.send(()).expect("finish hourly");
        while !archive_tasks.is_empty() {
            let result = archive_tasks.join_next().await;
            handle_archive_task_join(&state, &mut active_kinds, result)
                .await
                .expect("handle archive task");
        }
        assert!(active_kinds.is_empty());
    }

    #[tokio::test]
    async fn archive_cycle_does_not_start_duplicate_kind_while_active() {
        let config = Config::try_parse_from([
            "superbank-solparq",
            "--db-server",
            "127.0.0.1",
            "--db-user",
            "admin",
            "--db-password",
            "secret",
            "--archive-range-type",
            "custom:500",
            "--server-mode",
        ])
        .expect("valid config");
        let state = AppState::new();
        let shutdown = ShutdownToken::new();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();
        let finish_rx = Arc::new(Mutex::new(Some(finish_rx)));
        let run_once: ArchiveRunFactory = Arc::new(move |kind| {
            let started_tx = started_tx.clone();
            let finish_rx = finish_rx.clone();
            Box::pin(async move {
                started_tx.send(kind).expect("record started archive");
                let finish_rx = finish_rx.lock().expect("finish lock").take();
                if let Some(finish_rx) = finish_rx {
                    let _ = finish_rx.await;
                }
                Ok(ArchiveRunReport::skipped(kind, "test".to_string(), "test"))
            }) as ArchiveRunFuture
        });
        let mut active_kinds = HashSet::new();
        let mut archive_tasks = JoinSet::new();

        schedule_archive_cycle(
            &config,
            &state,
            &shutdown,
            &mut active_kinds,
            &mut archive_tasks,
            run_once.clone(),
        )
        .expect("first schedule");
        schedule_archive_cycle(
            &config,
            &state,
            &shutdown,
            &mut active_kinds,
            &mut archive_tasks,
            run_once,
        )
        .expect("second schedule");

        assert_eq!(
            started_rx.recv().await,
            Some(ArchiveKind::Custom { slots: 500 })
        );
        let duplicate = tokio::time::timeout(Duration::from_millis(50), started_rx.recv()).await;
        assert!(duplicate.is_err(), "same archive kind should not overlap");
    }

    #[tokio::test]
    async fn second_shutdown_signal_aborts_graceful_wait() {
        let mut archive_task = tokio::spawn(async { std::future::pending::<Result<()>>().await });
        let (force_tx, force_rx) = oneshot::channel();
        force_tx.send(()).expect("send forced shutdown signal");

        let outcome = wait_for_archive_task_or_forced_signal(&mut archive_task, async {
            force_rx.await.expect("forced shutdown signal");
            Ok(ShutdownSignal::CtrlC)
        })
        .await
        .expect("forced shutdown should be handled");

        assert_eq!(outcome, ShutdownWaitOutcome::Forced);
    }
}
