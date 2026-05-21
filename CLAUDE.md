# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

---

# Rust Architecture Rules

## 5. Keep `main.rs` Minimal

`main.rs` should only:
- load configuration
- initialize tracing/logging
- initialize runtime
- construct top-level services
- call `run()`

Do not place business logic directly in `main.rs`.

Prefer:
```rust
#[tokio::main]
async fn main() {
    let app = App::new();
    app.run().await;
}
```

## 6. Prefer Workspace + Multi-Crate Architecture

Large subsystems should be separate crates.

Preferred structure:
```text
crates/
    comms/
    runtime/
    storage/
    logging/

apps/
    gateway/
    sentinel/
```

## 7. Tokio / Async Rules

Prefer async-first architecture.

Use:
- `tokio::spawn` for background tasks
- channels for task communication
- structured shutdown mechanisms

Avoid:
- blocking operations inside async contexts
- uncontrolled task spawning
- global mutable async state

Long-running systems should support graceful shutdown.

## 8. Trait-Based Design

Use traits for subsystem boundaries when abstraction is useful.

Good candidates:
- storage backends
- communication transports
- runtime adapters
- plugin-style systems

Avoid traits for:
- single-use local logic
- premature abstraction
- simple data containers

## 9. Logging Rules

Use `tracing` instead of `println!`.

Prefer:
- structured logging
- spans for async task context
- explicit error context

Avoid:
- debug-only println logging
- inconsistent logging styles

## 10. Error Handling

Prefer:
- `thiserror` for library errors
- `anyhow` for application-level errors

Avoid:
- `.unwrap()` in production paths
- silent error swallowing

## 11. Module Boundaries

Separate:
- protocol layer
- business logic
- runtime orchestration
- storage layer

Avoid tightly coupling:
- transport + business logic
- logging + protocol handling
- runtime + storage internals

## 12. Avoid Overengineering

Do not introduce:
- unnecessary generics
- deeply nested abstractions
- plugin systems without clear need
- macros where normal Rust is clearer

Prefer straightforward code over framework-style architecture.
