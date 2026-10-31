pub mod aggregator;
pub mod matcher;
pub mod models;
#[cfg(feature = "rithmic")]
pub mod rithmic_ledger;
#[cfg(feature = "rithmic")]
pub mod rithmic_market;
#[cfg(feature = "rithmic")]
pub mod rithmic_order;
pub mod scaled;
pub(crate) mod spot_ledger;
pub mod volume_profile;

#[cfg(test)]
pub(crate) fn embedded_python_project_import_lock() -> std::sync::MutexGuard<'static, ()> {
    static PROJECT_IMPORT_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    PROJECT_IMPORT_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
