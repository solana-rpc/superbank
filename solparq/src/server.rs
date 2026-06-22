use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use tokio::{net::TcpListener, task::JoinSet};
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info};

use crate::{
    archive,
    config::Config,
    metrics::{AppState, ArchiveEvent, PublicStatus},
};

pub async fn run(config: Config) -> Result<()> {
    let state = AppState::new();
    let mut servers = JoinSet::new();

    servers.spawn(serve_ops(config.ops_port, state.clone(), config.clone()));
    servers.spawn(serve_metrics(config.metrics_port, state.clone()));

    info!(
        ops_port = config.ops_port,
        metrics_port = config.metrics_port,
        "solparq server mode started"
    );

    let loop_state = state.clone();
    let loop_config = config.clone();
    servers.spawn(async move { archive_loop(loop_config, loop_state).await });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
        }
        result = servers.join_next() => {
            if let Some(result) = result {
                result??;
            }
        }
    }
    servers.abort_all();
    Ok(())
}

async fn archive_loop(config: Config, state: Arc<AppState>) -> Result<()> {
    let mut interval =
        tokio::time::interval(Duration::from_secs(config.archive_check_interval_secs));
    loop {
        interval.tick().await;
        info!(
            archive_types = config.archive_kinds.len(),
            "checking for archiving tasks"
        );
        for kind in config.archive_kinds.iter().copied() {
            state.record_check_started(kind, None);
            info!(archive_kind = kind.to_string(), "checking archive task");
            match archive::run_once_for_kind(&config, kind).await {
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
    if status.last_error.is_none() {
        (
            StatusCode::OK,
            Json(json!({"status":"ok","last_success_at_unix":status.last_success_at_unix})),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"error","last_error":status.last_error})),
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
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
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
    let (health_label, health_class) = if status.last_error.is_none() {
        ("Healthy", "ok")
    } else {
        ("Needs attention", "bad")
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
    let timeline = render_timeline(&status.recent_events);
    let event_rows = render_event_rows(&status.recent_events);
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
    main {{ max-width: 1180px; margin: 0 auto; padding: 28px; }}
    header {{ display: flex; justify-content: space-between; gap: 20px; align-items: flex-start; margin-bottom: 24px; }}
    h1 {{ margin: 0 0 6px; font-size: 28px; font-weight: 720; }}
    h2 {{ margin: 0 0 14px; font-size: 17px; font-weight: 680; }}
    p {{ margin: 0; color: var(--muted); }}
    code {{ background: #eef3f8; border: 1px solid var(--line); padding: 2px 6px; border-radius: 5px; }}
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
    .two {{ display: grid; gap: 18px; grid-template-columns: minmax(0, 1.2fr) minmax(320px, .8fr); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ width: 210px; color: var(--muted); font-weight: 620; }}
    tr:last-child th, tr:last-child td {{ border-bottom: 0; }}
    .timeline svg {{ width: 100%; height: auto; display: block; }}
    .events {{ margin-top: 12px; max-height: 260px; overflow: auto; border-top: 1px solid var(--line); }}
    .event {{ display: grid; grid-template-columns: 164px 78px 1fr; gap: 10px; padding: 10px 0; border-bottom: 1px solid var(--line); font-size: 14px; }}
    .event:last-child {{ border-bottom: 0; }}
    .badge {{ display: inline-flex; justify-content: center; border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 700; color: #fff; }}
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
        <p>Auto-refreshes every 30 seconds</p>
      </div>
      <div class="pill {health_class}"><span class="dot"></span>{health_label}</div>
    </header>

    <div class="grid">
      <div class="metric"><div class="label">Slots available</div><div class="value">{db_slots}</div></div>
      <div class="metric"><div class="label">Archives created</div><div class="value">{archives_created}</div></div>
      <div class="metric"><div class="label">Last run</div><div class="value">{last_run}</div></div>
      <div class="metric"><div class="label">Last success</div><div class="value">{last_success}</div></div>
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
          <tr><th>Archive types</th><td>{archive_types}</td></tr>
          <tr><th>Location</th><td>{location}</td></tr>
          <tr><th>Output</th><td><code>{output}</code></td></tr>
          <tr><th>Transactions table</th><td><code>{transactions_table}</code></td></tr>
          <tr><th>Blocks table</th><td><code>{blocks_table}</code></td></tr>
          <tr><th>Archives to keep</th><td>{archives_to_keep}</td></tr>
          <tr><th>Continue from last archive</th><td>{continue_from_last_archive}</td></tr>
          <tr><th>Check interval</th><td>{check_interval} seconds</td></tr>
          <tr><th>Skipped</th><td>{archives_skipped}</td></tr>
          <tr><th>Errors</th><td>{archive_errors}</td></tr>
          <tr><th>Last error</th><td>{last_error}</td></tr>
        </table>
      </section>
    </div>
  </main>
</body>
</html>"#,
        health_class = health_class,
        health_label = health_label,
        db_slots = html_escape(&db_slots),
        archives_created = format_u64(status.archives_created),
        last_run = html_escape(&format_utc_timestamp(status.last_run_at_unix)),
        last_success = html_escape(&format_utc_timestamp(status.last_success_at_unix)),
        timeline = timeline,
        event_rows = event_rows,
        archive_types = html_escape(&archive_types),
        location = html_escape(&format!("{:?}", config.archive_location)),
        output = html_escape(&config.output_location.display().to_string()),
        transactions_table = html_escape(&config.transactions_table),
        blocks_table = html_escape(&config.blocks_table),
        archives_to_keep = format_u64(config.archives_to_keep as u64),
        continue_from_last_archive = config.continue_from_last_archive,
        check_interval = format_u64(config.archive_check_interval_secs),
        archives_skipped = format_u64(status.archives_skipped),
        archive_errors = format_u64(status.archive_errors),
        last_error = html_escape(status.last_error.as_deref().unwrap_or("none")),
    )
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
            format!(
                "<div class=\"event\"><div>{}</div><div><span class=\"badge {}\">{}</span></div><div><strong>{}</strong> {}</div></div>",
                html_escape(&format_utc_timestamp(Some(event.timestamp_unix))),
                html_escape(&event.outcome),
                html_escape(&event.outcome),
                html_escape(&event.archive_kind),
                html_escape(detail)
            )
        })
        .collect::<String>()
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
