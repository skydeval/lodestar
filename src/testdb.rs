//! A Postgres database for the tests that need one. Set
//! `LODESTAR_TEST_DATABASE_URL` to a database the tests may create schemas
//! in. Without it those tests return at once and pass: the unit tests run
//! anywhere, and CI sets the variable.

use std::sync::atomic::{AtomicU32, Ordering};

use tokio_postgres::Client;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A connection whose search path is a new, empty schema of its own, or
/// None when no test database is configured.
pub async fn empty() -> Option<Client> {
    let url = std::env::var("LODESTAR_TEST_DATABASE_URL").ok()?;
    let client = crate::db::connect(&url)
        .await
        .expect("connecting to the test database");
    let schema = format!(
        "test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; SET search_path TO {schema}"
        ))
        .await
        .expect("creating a test schema");
    Some(client)
}

/// As `empty`, with lodestar's schema created in it.
pub async fn fresh() -> Option<Client> {
    let client = empty().await?;
    crate::db::init(&client)
        .await
        .expect("creating lodestar's schema");
    Some(client)
}
