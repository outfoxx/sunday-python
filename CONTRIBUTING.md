# Contributing

Public APIs must be typed and documented. Runtime behavior should be covered with unit tests and adapter behavior with
integration tests. Run checks from fastest to slowest:

```shell
mise run format-check
mise run lint
mise run typecheck
mise run test
mise run build
uv run twine check dist/*
```

Release versions are derived from Git tags. Sunday-style tags such as `2.0.0-beta.1` normalize to the PEP 440 version
`2.0.0b1`. Publishing remains disabled unless the repository's protected `pypi` environment and
`PYPI_PUBLISH_ENABLED=true` variable are both configured.
