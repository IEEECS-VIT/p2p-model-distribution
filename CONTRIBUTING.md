# Contribution Guidelines

Thank you for contributing to IEEE CS projects.

This document outlines the contribution process, workflow, and governance standards that must be followed for all IEEE CS repositories.

---

## Repository Purpose

All IEEE CS repositories should be created using the official template to ensure:

- Consistent documentation
- Standardized contribution workflow
- Structured issue tracking
- Governance and branch discipline

Teams must customize project-specific details after initializing from the template.

---

## How To Use The Template

When creating a new project from the template:

1. Replace the contents of `README.md` with project-specific information.
2. Fill in all placeholder sections.
3. Define the project’s tech stack and architecture.
4. Update `.env.example` with required variables.
5. Modify CI workflows according to the stack.

---

## Branching Strategy

All repositories must follow the branching workflow below:

`feature/* → dev → main`


### Branch Types

- `main` → Protected production branch
- `dev` → Integration branch
- `feature/<name>` → New features
- `fix/<name>` → Bug fixes
- `chore/<name>` → Maintenance tasks

### Rules

- Direct commits to `main` are not allowed.
- All changes must go through Pull Requests.
- At least one reviewer approval is required before merging.
- CPull Requests must require review from designated Code Owners.

---

## Contribution Workflow

1. Create a branch from `dev` (or `main` if `dev` is not yet established).
2. Follow branch naming conventions.
3. Make clear and meaningful commits.
4. Open a Pull Request.
5. Wait for at least one approval before merging.

Keep Pull Requests small and focused.

---

## Environment Policy

- `.env` files must never be committed.
- `.env.example` should define required variables without values.
- Production secrets must be stored in deployment platform dashboards.
- Access to production credentials should be restricted to maintainers.

---

## Containerization Policy

All IEEE CS projects must provide a Docker setup.

Each project repository should include:

- A valid `Dockerfile`
- A `docker-compose.yml` (if multiple services are involved)
- Clear instructions for building and running containers

Projects are responsible for customizing Docker configuration according to their stack.

---

## Code Standards

- Do not commit secrets.
- Keep documentation updated.
- Follow project-specific style guidelines.
- Keep Pull Requests focused and small.

---

## Communication

- Use structured issue templates.
- Provide sufficient context in Pull Requests.
- Maintain professional and clear communication.
