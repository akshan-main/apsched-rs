use tracing_subscriber::prelude::*;

/// Initialize structured tracing for the scheduler.
///
/// When the `otel` feature is enabled and `otel_endpoint` is provided,
/// traces are also exported via OTLP.  Otherwise, only the
/// `tracing_subscriber::fmt` layer is used.
pub fn init_tracing(_otel_endpoint: Option<&str>) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("apsched=info"));

    let fmt_layer = tracing_subscriber::fmt::layer();

    #[cfg(feature = "otel")]
    {
        if let Some(endpoint) = _otel_endpoint {
            use opentelemetry::trace::TracerProvider;
            use opentelemetry_otlp::WithExportConfig;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("failed to create OTLP exporter");

            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .build();

            let tracer = provider.tracer("apsched");
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_layer)
                .init();

            return;
        }
    }

    // Default: stdout only
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}
