//! The route-ban probe: EVERY route surface this module can compose is
//! READ-ONLY (all three models are `read_only: true`, so even
//! `all_crud_routes()` mounts only GET endpoints — no write route exists
//! to mount), and the banned webhook route family (`/web/hook/<uuid>`)
//! is absent everywhere.
//!
//! Automation mutation happens ONLY through the guarded rule service
//! (host-side); webhook intake is not this module's to offer (it lives
//! under the ADR-0021 webhook module's own contract).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::common::{check, module_on, TestDb};

async fn status_of(router: &axum::Router, method: &str, uri: &str) -> StatusCode {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap_or_else(|e| panic!("PROBE-FAIL: request build failed: {e}"));
    let response = router
        .clone()
        .oneshot(request)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: router call {method} {uri} failed: {e}"));
    response.status()
}

#[tokio::test]
async fn every_composable_surface_is_read_only_and_hookless() {
    let db = TestDb::new("noroutes").await;
    let module = module_on(&db.pool).await;

    // The MAXIMAL surface this module can produce: all_crud_routes().
    // (readonly_routes() is a subset of it, so proving it here covers
    // every mountable surface.)
    let maximal = module.all_crud_routes();
    let readonly = module.readonly_routes();

    for (label, router) in [("all_crud_routes", &maximal), ("readonly_routes", &readonly)] {
        // Reads exist (the surface is not empty).
        let list = status_of(router, "GET", "/automation_rules").await;
        check(
            list != StatusCode::NOT_FOUND && list != StatusCode::METHOD_NOT_ALLOWED,
            &format!("{label}: GET /automation_rules is a mounted read"),
        );

        // No mutation route exists on any entity.
        for (method, uri) in [
            ("POST", "/automation_rules"),
            ("PUT", "/automation_rules/00000000-0000-0000-0000-000000000001"),
            ("PATCH", "/automation_rules/00000000-0000-0000-0000-000000000001"),
            ("DELETE", "/automation_rules/00000000-0000-0000-0000-000000000001"),
            ("POST", "/automation_runs"),
            ("POST", "/automation_rules/upsert"),
            ("DELETE", "/automation_runs/00000000-0000-0000-0000-000000000001"),
            ("POST", "/scheduler_postures"),
            ("PUT", "/scheduler_postures/00000000-0000-0000-0000-000000000001"),
            ("POST", "/scheduler_postures/bulk"),
        ] {
            let status = status_of(router, method, uri).await;
            check(
                status == StatusCode::METHOD_NOT_ALLOWED,
                &format!("{label}: {method} {uri} has no route (405; read-only models mount reads only)"),
            );
        }

        // The banned webhook family is absent entirely.
        for uri in [
            "/web/hook/00000000-0000-0000-0000-000000000002",
            "/web/hook/anything-at-all",
            "/web/hook/",
        ] {
            let status = status_of(router, "GET", uri).await;
            check(
                status == StatusCode::NOT_FOUND,
                &format!("{label}: GET {uri} is 404 (no public automation/webhook route exists)"),
            );
            let post = status_of(router, "POST", uri).await;
            check(
                post == StatusCode::NOT_FOUND,
                &format!("{label}: POST {uri} is 404 (webhook intake is not this module's)"),
            );
        }
    }

    db.dispose().await;
}
