# Development

## Prerequisites

This project uses [mise](https://mise.jdx.dev/) to pin the Go toolchain and every
dev tool. Install mise once, then everything else comes from `mise.toml`:

- Go 1.27
- [Task](https://taskfile.dev/) — command runner
- [pre-commit](https://pre-commit.com/) — git-hooks framework
- [golangci-lint](https://golangci-lint.run/) v2 — meta-linter
- [gitleaks](https://github.com/gitleaks/gitleaks) — secret scan
- [gofumpt](https://github.com/mvdan/gofumpt), [goimports](https://pkg.go.dev/golang.org/x/tools/cmd/goimports), [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) — installed as project-local Go binaries

## First-time setup

```shell
mise install     # or: task tools
task setup       # installs mise tools + registers pre-commit + pre-push hooks
```

That's it. Every subsequent commit is auto-formatted, vetted, and linted; every
push is vuln-scanned.

## Common tasks

Everything runs through Task. `task --list` shows all targets.

| Command              | What it does                                         |
| -------------------- | ---------------------------------------------------- |
| `task build`         | Build **linux + windows** binaries into `./bin/`     |
| `task build-linux`   | Just `./bin/xy3`                                     |
| `task build-windows` | Just `./bin/xy3.exe`                                 |
| `task fmt`           | gofumpt + goimports                                  |
| `task vet`           | `go vet ./...`                                       |
| `task lint`          | Full golangci-lint suite (config: `.golangci.yml`)   |
| `task lint-fast`     | Fast preset — same set that pre-commit runs          |
| `task test`          | `go test ./...`                                      |
| `task test-race`     | `go test -race ./...`                                |
| `task cover`         | Coverage report                                      |
| `task vuln`          | `govulncheck ./...`                                  |
| `task tidy`          | `go mod tidy`                                        |
| `task ci`            | fmt + vet + lint + test-race + vuln                  |
| `task clean`         | Remove `./bin/` and `coverage.out`                   |

## Git hooks

`task setup` (once per clone) wires up hooks via `pre-commit`.

**Runs on every commit** (`.pre-commit-config.yaml`):
- `gofumpt` and `goimports` on staged Go files
- `go vet ./...`
- `go mod tidy` drift check (fails if `go.mod` / `go.sum` need tidying)
- `golangci-lint run --fast-only`
- `gitleaks` secret scan
- trailing-whitespace / EOL / merge-marker hygiene

**Runs on `git push`** (kept off the commit path because it hits the vuln DB):
- `govulncheck ./...`

Bump the pinned hook versions when you feel like it:

```shell
pre-commit autoupdate
```

## Pushing changes to GitHub

To push through a Personal Access Token:

```shell
GITHUB_PAT=github_pat_123; git remote set-url origin $(git config --get remote.origin.url | perl -F'github.com' -sanE 'print "https://${token}\@github.com$F[1]"' -- -token=$GITHUB_PAT;)
```
