dq_rules:
  - rule: completeness
    params:
      column: client_id
    threshold: 0.99
    severity: FAIL

  - rule: uniqueness
    params:
      column: client_id
    threshold: 0.98
    severity: WARN

  - rule: validity
    params:
      column: country_code
      allowed_values: ["NL", "DE", "BE"]
    threshold: 0.97
    severity: CONTINUE
