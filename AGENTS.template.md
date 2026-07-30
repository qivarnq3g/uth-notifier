# Repository agent prompt template

## Reliability-first optimization

Use this prompt when requesting a repository change:

> Work toward the most optimized and stable solution. Do not limit the solution's complexity or difficulty; prioritize correctness, reliability, security, observability, predictable resource use, rollback, and zero mandatory cost. Do not choose a weaker solution merely because it is easier to implement. State every trade-off explicitly, validate it with data, and preserve contract compatibility and production operability.

Apply the prompt together with the repository's `AGENTS.md`, security boundaries, platform terms, versioned contracts, verification commands, and rollback procedures. Optimization does not authorize secrets, authentication bypass, destructive actions, paid services, or unrelated scope expansion.
