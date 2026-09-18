// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

async fn assert_invalid_params(response: Response, message: &str) {
    assert_eq!(response.status(), StatusCode::OK);
    let body = parse_json_rpc_response(response).await;
    let error = body.error.expect("invalid params");
    assert_eq!(error.code, -32602);
    assert_eq!(error.message, message);
    assert!(error.data.is_none());
}

#[tokio::test]
async fn inflation_limit_uses_configured_agave_error_before_address_parsing() {
    for limit in [32, 100] {
        let mut state = test_state();
        Arc::get_mut(&mut state)
            .unwrap()
            .get_inflation_reward_max_addresses = Some(limit);
        let response = handle_get_inflation_reward(
            state,
            json!(1),
            Some(vec![json!(vec!["invalid"; limit + 1])]),
        )
        .await
        .unwrap();
        assert_invalid_params(response, &format!("Too many inputs provided; max {limit}")).await;
    }
}

#[tokio::test]
async fn inflation_boundary_and_disabled_limit_reach_address_validation() {
    for (limit, count) in [
        (Some(32), 31),
        (Some(32), 32),
        (Some(100), 100),
        (None, 101),
    ] {
        let mut state = test_state();
        Arc::get_mut(&mut state)
            .unwrap()
            .get_inflation_reward_max_addresses = limit;
        let response =
            handle_get_inflation_reward(state, json!(1), Some(vec![json!(vec!["invalid"; count])]))
                .await
                .unwrap();
        assert_invalid_params(response, "Invalid param: Invalid").await;
    }
}
