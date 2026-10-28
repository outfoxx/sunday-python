# RFC 9457 standard members

`ProblemPayload` and its subclasses ignore JSON null and wrong-category values for `type`, `title`, `status`, `detail`, and `instance` before field validation. String members accept only strings; status accepts numbers, excludes booleans, and still rejects fractional or unrepresentable integer values. Ordinary `SundayModel` types and extension members retain their existing rules.

Direct decoding of `{}` yields `type="about:blank"` and absent optional standard members. The registry chooses the effective type before dispatch. A missing or discarded type never selects a custom registered problem. Typed payload constraints still reject `about:blank` where a custom fixed type is required.

With HTTP context, absent status uses the response status. Valid body status wins. Missing titles receive the status reason only for `about:blank`. Explicit empty strings are retained. Serialization omits absent standard members and preserves nullable extension fields, including through Litestar.

Normalization is a copied-mapping operation used by an inherited Pydantic `before` model validator. Discarded optional members are not marked as explicitly set. The effective type is materialized before validation so inherited custom defaults cannot replace `about:blank`. Pydantic handles defaults, constraints, field serializers, requiredness, and field presence normally; no Problem-specific core-schema rewriting or validation state is used.

The five standard members have fixed RFC wire names. Subclasses may specialize their types, constraints, requiredness, and defaults, but may not rename them with validation or serialization aliases, including path aliases. Extensions may use normal Pydantic aliases and paths, provided they do not claim a reserved standard-member name. Invalid configurations raise `TypeError` when the subclass is defined. This deliberately replaces the earlier prerelease's support for remapping standard members; use an application adapter before decoding if another input format needs translation into RFC Problem Details.

The alias restriction is Sunday's API policy, not an RFC requirement about Python classes. The RFC governs wire names and values; it does not specify language-level property names or aliases. Likewise, required fields in a specialized payload are authored constraints; the RFC itself allows omitted title and detail.

Only mapping/JSON input is normalized. Pydantic's attribute-based validation retains its native behavior. Nullable extensions, including nested objects with keys such as `detail`, are preserved. A small native wrapping serializer omits null standard members even when a subclass redeclares a field without the base `exclude_if` option.

Companion Generator output calls normalization before its own pre-validation checks. Regenerate Problems when adopting the companion Generator release; ordinary models need no migration.

Reference: [RFC 9457 §3.1](https://www.rfc-editor.org/rfc/rfc9457.html#section-3.1).
