use std::path::PathBuf;

use opentelemetry::sdk::export::metrics::aggregation;
use opentelemetry::sdk::metrics::{controllers, processors, selectors};
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_api::admin::api_server::AdminApiServer;
use garage_api::s3::api_server::S3ApiServer;
use garage_model::garage::Garage;
use garage_web::WebServer;

#[cfg(feature = "k2v")]
use garage_api::k2v::api_server::K2VApiServer;

use crate::admin::*;
#[cfg(feature = "telemetry-otlp")]
use crate::tracing_setup::*;

async fn wait_from(mut chan: watch::Receiver<bool>) {
	while !*chan.borrow() {
		if chan.changed().await.is_err() {
			return;
		}
	}
}

pub async fn run_server(config_file: PathBuf) -> Result<(), Error> {
	info!("Loading configuration...");
	let config = read_config(config_file)?;

	// ---- Initialize Garage internals ----

	#[cfg(feature = "metrics")]
	let controller = controllers::basic(
		processors::factory(
			selectors::simple::histogram([1.0, 2.0, 5.0, 10.0, 20.0, 50.0]),
			aggregation::cumulative_temporality_selector(),
		)
		.with_memory(true),
	)
	.build();
	let metrics_exporter = opentelemetry_prometheus::exporter(controller).init();

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone())?;

	info!("Initializing background runner...");
	let watch_cancel = watch_shutdown_signal();
	let (background, await_background_done) = BackgroundRunner::new(watch_cancel.clone());

	info!("Spawning Garage workers...");
	garage.spawn_workers(&background);

	if config.admin.trace_sink.is_some() {
		info!("Initialize tracing...");

		#[cfg(feature = "telemetry-otlp")]
		init_tracing(config.admin.trace_sink.as_ref().unwrap(), garage.system.id)?;

		#[cfg(not(feature = "telemetry-otlp"))]
		error!("Garage was built without OTLP exporter, admin.trace_sink is ignored.");
	}

	info!("Initialize Admin API server and metrics collector...");
	let admin_server = AdminApiServer::new(
		garage.clone(),
		#[cfg(feature = "metrics")]
		metrics_exporter,
	);

	info!("Launching internal Garage cluster communications...");
	let run_system = tokio::spawn(garage.system.clone().run(watch_cancel.clone()));

	info!("Create admin RPC handler...");
	AdminRpcHandler::new(garage.clone(), background.clone());

	// ---- Launch public-facing API servers ----

	let mut servers = vec![];

	if let Some(s3_bind_addr) = &config.s3_api.api_bind_addr {
		info!("Initializing S3 API server...");
		servers.push((
			"S3 API",
			tokio::spawn(S3ApiServer::run(
				garage.clone(),
				*s3_bind_addr,
				config.s3_api.s3_region.clone(),
				wait_from(watch_cancel.clone()),
			)),
		));
	}

	if config.k2v_api.is_some() {
		#[cfg(feature = "k2v")]
		{
			info!("Initializing K2V API server...");
			servers.push((
				"K2V API",
				tokio::spawn(K2VApiServer::run(
					garage.clone(),
					config.k2v_api.as_ref().unwrap().api_bind_addr,
					config.s3_api.s3_region.clone(),
					wait_from(watch_cancel.clone()),
				)),
			));
		}
		#[cfg(not(feature = "k2v"))]
		error!("K2V is not enabled in this build, cannot start K2V API server");
	}

	if let Some(web_config) = &config.s3_web {
		info!("Initializing web server...");
		servers.push((
			"Web",
			tokio::spawn(WebServer::run(
				garage.clone(),
				web_config.bind_addr,
				web_config.root_domain.clone(),
				wait_from(watch_cancel.clone()),
			)),
		));
	}

	if let Some(admin_bind_addr) = &config.admin.api_bind_addr {
		info!("Launching Admin API server...");
		servers.push((
			"Admin",
			tokio::spawn(admin_server.run(*admin_bind_addr, wait_from(watch_cancel.clone()))),
		));
	}

	#[cfg(not(feature = "metrics"))]
	if config.admin.metrics_token.is_some() {
		warn!("This Garage version is built without the metrics feature");
	}

	// Stuff runs

	// When a cancel signal is sent, stuff stops

	// Collect stuff
	for (desc, join_handle) in servers {
		if let Err(e) = join_handle.await? {
			error!("{} server exited with error: {}", desc, e);
		} else {
			info!("{} server exited without error.", desc);
		}
	}

	// Remove RPC handlers for system to break reference cycles
	garage.system.netapp.drop_all_handlers();
	opentelemetry::global::shutdown_tracer_provider();

	// Await for netapp RPC system to end
	run_system.await?;
	info!("Netapp exited");

	// Drop all references so that stuff can terminate properly
	drop(garage);

	// Await for all background tasks to end
	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}

#[cfg(unix)]
fn watch_shutdown_signal() -> watch::Receiver<bool> {
	use tokio::signal::unix::*;

	let (send_cancel, watch_cancel) = watch::channel(false);
	tokio::spawn(async move {
		let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
		let mut sigterm =
			signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
		let mut sighup = signal(SignalKind::hangup()).expect("Failed to install SIGHUP handler");
		tokio::select! {
			_ = sigint.recv() => info!("Received SIGINT, shutting down."),
			_ = sigterm.recv() => info!("Received SIGTERM, shutting down."),
			_ = sighup.recv() => info!("Received SIGHUP, shutting down."),
		}
		send_cancel.send(true).unwrap();
	});
	watch_cancel
}

#[cfg(windows)]
fn watch_shutdown_signal() -> watch::Receiver<bool> {
	use tokio::signal::windows::*;

	let (send_cancel, watch_cancel) = watch::channel(false);
	tokio::spawn(async move {
		let mut sigint = ctrl_c().expect("Failed to install Ctrl-C handler");
		let mut sigclose = ctrl_close().expect("Failed to install Ctrl-Close handler");
		let mut siglogoff = ctrl_logoff().expect("Failed to install Ctrl-Logoff handler");
		let mut sigsdown = ctrl_shutdown().expect("Failed to install Ctrl-Shutdown handler");
		tokio::select! {
			_ = sigint.recv() => info!("Received Ctrl-C, shutting down."),
			_ = sigclose.recv() => info!("Received Ctrl-Close, shutting down."),
			_ = siglogoff.recv() => info!("Received Ctrl-Logoff, shutting down."),
			_ = sigsdown.recv() => info!("Received Ctrl-Shutdown, shutting down."),
		}
		send_cancel.send(true).unwrap();
	});
	watch_cancel
}
