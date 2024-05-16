# Development

## Pushing changes to GitHub

To be able to push changes, you need a GitHub Personal Access Token.
```shell
GITHUB_PAT=github_pat_1234 && git remote set-url origin "https://${GITHUB_PAT}@github.com/nguyengg/xy3.git"
```

## Git Hooks

Set up pre-commit Git hook that run `go fmt`:
```shell
git config core.hooksPath .githooks
```
