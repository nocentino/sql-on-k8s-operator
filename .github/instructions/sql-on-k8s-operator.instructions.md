# SQL on K8s Operator Project-Specific Copilot Instructions

These instructions are specific to the sql-on-k8s-operator project and supplement the default repository-wide Copilot instructions.

## Project Overview
This repository contains a Kubernetes operator for SQL-related workloads built with Kubebuilder and controller-runtime.

## Project Structure
- `cmd/main.go` — manager entrypoint
- `api/` — CRD API types and versioned schemas
- `internal/controller/` — reconciliation logic
- `internal/` — operator internals and supporting implementation
- `config/` — CRDs, RBAC, manifests, samples
- `dist/` — distribution artifacts
- `hack/` — helper scripts and development tooling
- `test/` / `testing/` — tests and related validation assets
- `Makefile` — build, generate, test, and deploy commands
- `PROJECT` — Kubebuilder project metadata

## Working Guidelines
- Respect Kubebuilder project structure and conventions
- Avoid manual edits to generated artifacts unless the workflow explicitly requires regeneration
- Prefer changes in source types, controller code, and configuration inputs, then regenerate derived files as needed
- Keep reconciliation logic idempotent and safe for repeated execution
- Match existing controller-runtime, logging, and API design patterns already established in the repo

## Operational Notes
- Changes to API types may require regeneration steps
- Changes to controller or marker-based configuration may impact CRDs and RBAC output
- Treat manifests, RBAC, CRDs, and controller behavior as connected parts of the same operator workflow
- Existing repository-level guidance in `AGENTS.md` should continue to be followed alongside the default instructions
