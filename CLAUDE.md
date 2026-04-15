# CLAUDE.md

## Project Overview
Python-based project with bot, database, and services components.

## Critical Rules

### Code Organization
- Many small files over few large files
- High cohesion, low coupling
- 200-400 lines typical, 800 max per file
- Organize by feature/domain, not by type

### Code Style
- No emojis in code, comments, or documentation
- Proper error handling with try/except
- Input validation on all user inputs
- Type hints on all functions
- Immutability preferred - avoid mutating objects/arrays

### Testing
- TDD: Write tests first (RED -> GREEN -> REFACTOR)
- 80% minimum coverage
- Unit tests for utilities
- Integration tests for APIs
- E2E tests for critical flows

### Security
- No hardcoded secrets - use environment variables
- Validate all user inputs
- Parameterized queries only
- No secrets in logs or error messages

## Git Workflow
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`
- Create separate commits per file - do NOT bundle multiple file changes
- Never commit to main directly
- PRs require review
- All tests must pass before merge

## Subagent Orchestration
- Use **planner** agent for complex feature implementation
- Use **code-reviewer** agent after writing code
- Use **tdd-guide** agent for new features and bug fixes
- Use **security-reviewer** agent before commits
- Use **architect** agent for architectural decisions
- Use **build-error-resolver** agent when builds fail
- Use **refactor-cleaner** agent for dead code cleanup
- Use **doc-updater** agent for documentation updates

## Workflow Best Practices
- Keep CLAUDE.md under 200 lines per file
- Use commands for workflows instead of standalone agents
- Create feature-specific subagents with skills (progressive disclosure)
- Perform manual `/compact` at ~50% context usage
- Start with plan mode for complex tasks
- Break subtasks small enough to complete in under 50% context

## Available Commands
- `/tdd` - Test-driven development workflow
- `/plan` - Create implementation plan
- `/code-review` - Review code quality
- `/build-fix` - Fix build errors
- `/learn` - Extract patterns from session

## Debugging Tips
- Use `/doctor` for diagnostics
- Run long-running terminal commands as background tasks
- Provide screenshots when reporting visual issues
