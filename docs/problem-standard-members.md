# RFC 9457 standard members

`ProblemPayload` and its subclasses ignore JSON null and wrong-category values for `type`, `title`, `status`, `detail`, and `instance` before field validation. String members accept only strings; status accepts numbers, excludes booleans, and still rejects fractional or unrepresentable integer values. Ordinary `SundayModel` types and extension members retain their existing rules.

Direct decoding of `{}` yields `type="about:blank"` and absent optional standard members. The registry chooses the effective type before dispatch. A missing or discarded type never selects a custom registered problem. Typed payload constraints still reject `about:blank` where a custom fixed type is required.

With HTTP context, absent status uses the response status. Valid body status wins. Missing titles receive the status reason only for `about:blank`. Explicit empty strings are retained. Serialization omits absent standard members and preserves nullable extension fields, including through Litestar.

Normalization copies input mappings and respects string aliases and alias choices. Discarded optional members are not marked as explicitly set. The effective type is materialized before validation so inherited custom defaults cannot replace `about:blank`.

Companion Generator output calls normalization before its own pre-validation checks. Regenerate Problems when adopting the companion Generator release; ordinary models need no migration.

Reference: [RFC 9457 §3.1](https://www.rfc-editor.org/rfc/rfc9457.html#section-3.1).
