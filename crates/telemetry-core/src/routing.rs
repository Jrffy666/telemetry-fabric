use crate::config::{ExporterConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::telemetry::SignalKind;

pub struct Router<'a> {
    config: &'a PipelineConfig,
}

impl<'a> Router<'a> {
    pub fn new(config: &'a PipelineConfig) -> Self {
        Self { config }
    }

    pub fn exporters_for(
        &self,
        signal: SignalKind,
    ) -> Result<Vec<&'a ExporterConfig>, PipelineError> {
        let route = self
            .config
            .routes
            .iter()
            .find(|route| route.signal == signal)
            .ok_or_else(|| PipelineError::MissingRoute(signal.to_string()))?;

        let mut exporters = Vec::with_capacity(route.exporters.len());
        for name in &route.exporters {
            let exporter = self
                .config
                .exporters
                .iter()
                .find(|exporter| exporter.name == *name)
                .ok_or_else(|| PipelineError::UnknownExporter(name.clone()))?;
            exporters.push(exporter);
        }
        Ok(exporters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PipelineConfig;

    #[test]
    fn resolves_default_trace_route() -> Result<(), Box<dyn std::error::Error>> {
        let config = PipelineConfig::default();
        let router = Router::new(&config);

        let exporters = router.exporters_for(SignalKind::Trace)?;

        assert_eq!(exporters.len(), 1);
        assert_eq!(exporters[0].name, "stdout");
        Ok(())
    }
}
