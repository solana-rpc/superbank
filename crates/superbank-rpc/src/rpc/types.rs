// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};
use serde_json::Value;
use std::borrow::Cow;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JsonRpcInboundRequest {
    pub(crate) jsonrpc: Option<String>,
    // Distinguish missing id (None) from explicit null (Some(None)).
    #[serde(default)]
    pub(crate) id: Option<Option<Value>>,
    pub(crate) method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JsonRpcRequest {
    pub(crate) jsonrpc: String,
    pub(crate) id: Value,
    pub(crate) method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<Vec<Value>>,
}

#[derive(Debug)]
pub(crate) struct JsonRpcResponse<T = Value> {
    pub(crate) jsonrpc: Cow<'static, str>,
    pub(crate) id: Value,
    pub(crate) result: Option<T>,
    pub(crate) error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JsonRpcError {
    pub(crate) code: i32,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
}

#[derive(Serialize, Deserialize)]
struct JsonRpcSuccessWire<J, I, T> {
    jsonrpc: J,
    id: I,
    result: T,
}

#[derive(Serialize, Deserialize)]
struct JsonRpcErrorWire<J, I, E> {
    jsonrpc: J,
    id: I,
    error: E,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum JsonRpcResponseWire<T> {
    Success(JsonRpcSuccessWire<String, Value, T>),
    Error(JsonRpcErrorWire<String, Value, JsonRpcError>),
}

impl<T> Serialize for JsonRpcResponse<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match (&self.result, &self.error) {
            (Some(result), None) => JsonRpcSuccessWire {
                jsonrpc: &self.jsonrpc,
                id: &self.id,
                result,
            }
            .serialize(serializer),
            (None, Some(error)) => JsonRpcErrorWire {
                jsonrpc: &self.jsonrpc,
                id: &self.id,
                error,
            }
            .serialize(serializer),
            _ => Err(S::Error::custom(
                "JSON-RPC response must contain exactly one of result or error",
            )),
        }
    }
}

impl<'de, T> Deserialize<'de> for JsonRpcResponse<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match JsonRpcResponseWire::<T>::deserialize(deserializer)? {
            JsonRpcResponseWire::Success(success) => Ok(Self {
                jsonrpc: Cow::Owned(success.jsonrpc),
                id: success.id,
                result: Some(success.result),
                error: None,
            }),
            JsonRpcResponseWire::Error(error) => Ok(Self {
                jsonrpc: Cow::Owned(error.jsonrpc),
                id: error.id,
                result: None,
                error: Some(error.error),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonRpcError, JsonRpcResponse};
    use serde_json::{Value, json};
    use std::borrow::Cow;

    #[test]
    fn json_rpc_response_serialization_rejects_missing_result_and_error() {
        let response = JsonRpcResponse::<Value> {
            jsonrpc: Cow::Borrowed("2.0"),
            id: json!(0),
            result: None,
            error: None,
        };

        let err = serde_json::to_value(&response).expect_err("expected serialization error");
        assert!(err.to_string().contains("exactly one of result or error"));
    }

    #[test]
    fn json_rpc_response_deserialization_rejects_missing_result_and_error() {
        serde_json::from_value::<JsonRpcResponse<Value>>(json!({
            "jsonrpc": "2.0",
            "id": 0
        }))
        .expect_err("expected deserialization error");
    }

    #[test]
    fn json_rpc_response_deserialization_accepts_error_variant() {
        let parsed = serde_json::from_value::<JsonRpcResponse<Value>>(json!({
            "jsonrpc": "2.0",
            "id": 0,
            "error": {
                "code": -32603,
                "message": "Internal error"
            }
        }))
        .expect("expected valid response");

        assert_eq!(parsed.id, json!(0));
        assert!(parsed.result.is_none());
        let err = parsed.error.expect("error");
        assert_eq!(
            err.code,
            JsonRpcError {
                code: -32603,
                message: "Internal error".to_string(),
                data: None
            }
            .code
        );
    }
}
