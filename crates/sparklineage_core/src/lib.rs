use pyo3::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

// Pre-compiled high performance regexes
static SECRET_URI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(jdbc:[a-z0-9_]+://[^:]+:)([^@\s]+)(@)").unwrap()
});

static S3_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(s3[a-z]?://)([^:]+:)([^@\s]+)(@)").unwrap()
});

static GENERIC_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(password|secret|api[_-]?key|token)\s*[=:]\s*['\x22]?([A-Za-z0-9_/\+=\-]{8,})['\x22]?").unwrap()
});

static PII_FILTER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(ssn|social_security|dob|date_of_birth|credit_card|card_num|password|secret)\b[#0-9A-Z_]*\s*([=><!]+)\s*(['\x22][^'\x22]+['\x22])").unwrap()
});

static ATTR_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"([A-Za-z0-9_]+)#(\d+)[L]?").unwrap()
});

static AS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(.*?)\s+AS\s+([A-Za-z0-9_]+)#(\d+)[L]?$").unwrap()
});

#[derive(Serialize, Deserialize)]
pub struct FastNode {
    pub id: String,
    pub name: String,
    pub details: String,
    pub output_columns: Vec<String>,
    pub children: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct FastPlan {
    pub nodes: Vec<FastNode>,
    pub root_id: String,
    pub column_lineage: HashMap<String, Vec<String>>,
}

/// Redacts sensitive credentials, database passwords, and PII literals from execution plans.
#[pyfunction]
pub fn redact_secrets(input: &str) -> String {
    let mut redacted = input.to_string();

    // 1. Redact JDBC credentials
    redacted = SECRET_URI_REGEX
        .replace_all(&redacted, "${1}********${3}")
        .to_string();

    // 2. Redact S3/Storage credentials
    redacted = S3_KEY_REGEX
        .replace_all(&redacted, "${1}${2}********${4}")
        .to_string();

    // 3. Redact generic keys/tokens
    redacted = GENERIC_KEY_REGEX
        .replace_all(&redacted, "${1}=********")
        .to_string();

    // 4. Redact PII in filter comparisons
    redacted = PII_FILTER_REGEX
        .replace_all(&redacted, "${1} ${2} '***REDACTED_PII***'")
        .to_string();

    redacted
}

/// Scans text for security vulnerabilities and returns a list of (rule_id, severity, message).
#[pyfunction]
pub fn scan_security(input: &str) -> Vec<(String, String, String)> {
    let mut issues = Vec::new();

    if SECRET_URI_REGEX.is_match(input) {
        issues.push((
            "SEC01_JDBC_CREDENTIAL_LEAK".to_string(),
            "CRITICAL".to_string(),
            "Hardcoded database credentials detected in connection URI inside Catalyst plan.".to_string(),
        ));
    }

    if S3_KEY_REGEX.is_match(input) {
        issues.push((
            "SEC02_STORAGE_SECRET_LEAK".to_string(),
            "CRITICAL".to_string(),
            "Cloud storage access keys/secrets detected in storage path inside Catalyst plan.".to_string(),
        ));
    }

    if GENERIC_KEY_REGEX.is_match(input) {
        issues.push((
            "SEC03_API_KEY_LEAK".to_string(),
            "CRITICAL".to_string(),
            "Unmasked API key or token found in plan configuration options.".to_string(),
        ));
    }

    if PII_FILTER_REGEX.is_match(input) {
        issues.push((
            "SEC04_PII_FILTER_EXPOSURE".to_string(),
            "WARNING".to_string(),
            "Sensitive PII literal (SSN, DOB, Card, Password) unmasked in filter predicate.".to_string(),
        ));
    }

    issues
}

/// Ultra-fast offline static scanner for PySpark code (e.g. CI pre-commit checks).
/// Returns list of (rule_id, severity, line_number, message).
#[pyfunction]
pub fn scan_code(code: &str) -> Vec<(String, String, usize, String)> {
    let mut issues = Vec::new();
    let lines: Vec<&str> = code.lines().collect();
    let mut in_loop = false;

    for (idx, line) in lines.iter().enumerate() {
        let line_num = idx + 1;
        let trimmed = line.trim();

        if trimmed.starts_with("for ") || trimmed.starts_with("while ") {
            in_loop = true;
        } else if !line.starts_with(" ") && !line.starts_with("\t") {
            in_loop = false;
        }

        // Rule 1: .collect() detection
        if trimmed.contains(".collect()") {
            if in_loop {
                issues.push((
                    "CODE01_COLLECT_IN_LOOP".to_string(),
                    "CRITICAL".to_string(),
                    line_num,
                    "Calling .collect() inside a loop pulls distributed partitions into single-node driver memory repeatedly, risking driver OOM.".to_string(),
                ));
            } else {
                issues.push((
                    "CODE01_COLLECT_USAGE".to_string(),
                    "INFO".to_string(),
                    line_num,
                    "Consider .take(N) or .limit(N) instead of .collect() to avoid pulling full dataset to driver.".to_string(),
                ));
            }
        }

        // Rule 2: .crossJoin() without condition
        if trimmed.contains(".crossJoin(") {
            issues.push((
                "CODE02_EXPLICIT_CROSS_JOIN".to_string(),
                "WARNING".to_string(),
                line_num,
                "Explicit crossJoin() causes O(N*M) Cartesian data explosion. Verify table sizes before executing.".to_string(),
            ));
        }

        // Rule 3: toPandas() on un-limited DataFrame
        if trimmed.contains(".toPandas()") && !trimmed.contains(".limit(") {
            issues.push((
                "CODE03_UNBOUNDED_TO_PANDAS".to_string(),
                "WARNING".to_string(),
                line_num,
                "Calling .toPandas() without .limit() pulls all distributed data to single-node memory.".to_string(),
            ));
        }

        // Rule 4: Type casting in filter predicates
        if trimmed.contains(".filter(") && trimmed.contains("cast(") {
            issues.push((
                "CODE04_CAST_IN_FILTER".to_string(),
                "WARNING".to_string(),
                line_num,
                "Type casting column inside .filter() prevents partition pushdown and Parquet file skipping.".to_string(),
            ));
        }
    }

    issues
}

/// Lightning-fast Catalyst plan string parser in Rust.
/// Returns JSON string of FastPlan.
#[pyfunction]
pub fn fast_parse_plan_json(tree_str: &str) -> PyResult<String> {
    let mut nodes = Vec::new();
    let mut col_lineage: HashMap<String, Vec<String>> = HashMap::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut node_idx = 0;
    let mut root_id = String::new();

    for line in tree_str.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Detect indent
        let indent = line.len() - line.trim_start().len();
        let node_id = format!("rust_node_{}", node_idx);
        node_idx += 1;

        if root_id.is_empty() {
            root_id = node_id.clone();
        }

        // Extract node name
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let name = parts.get(0).unwrap_or(&"Node").to_string();

        let mut out_cols = Vec::new();
        for cap in ATTR_REGEX.captures_iter(trimmed) {
            let cname = cap[1].to_string();
            if !out_cols.contains(&cname) {
                out_cols.push(cname.clone());
                col_lineage.entry(cname).or_default().push(name.clone());
            }
        }

        while let Some(top) = stack.last() {
            if top.0 >= indent {
                stack.pop();
            } else {
                break;
            }
        }

        let parent_id = stack.last().map(|s| s.1.clone());
        stack.push((indent, node_id.clone()));

        nodes.push(FastNode {
            id: node_id,
            name,
            details: trimmed.to_string(),
            output_columns: out_cols,
            children: parent_id.map(|p| vec![p]).unwrap_or_default(),
        });
    }

    let plan = FastPlan {
        nodes,
        root_id,
        column_lineage: col_lineage,
    };

    serde_json::to_string(&plan).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("JSON serialization failed: {}", e))
    })
}

#[pymodule]
fn sparklineage_core(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(redact_secrets, m)?)?;
    m.add_function(wrap_pyfunction!(scan_security, m)?)?;
    m.add_function(wrap_pyfunction!(scan_code, m)?)?;
    m.add_function(wrap_pyfunction!(fast_parse_plan_json, m)?)?;
    Ok(())
}
