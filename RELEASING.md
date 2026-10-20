# Releasing

All packages share one version. Publishing runs in GitHub Actions (`.github/workflows/release.yml`) with **trusted publishing** — the registries trust this repository's workflow through GitHub OIDC, so no registry token is stored here.

## One-time setup

| registry | package | what to configure |
|---|---|---|
| npm | `soritrading-watch-together` | The first version must be published once by hand (`npm publish` in `js/`, logged in with 2FA). Then: package **Settings → Trusted publishing → GitHub Actions**: owner `valueconnectinc`, repository `soritrading-watch-together`, workflow `release.yml`, allow **npm publish**. A new configuration must be used within 2 days. |
| PyPI | `soritrading-watch-together` | **Publishing → Add a pending publisher** (works before the project exists): owner `valueconnectinc`, repository `soritrading-watch-together`, workflow `release.yml`, environment empty. |
| crates.io | `soritrading-watch-together` | The first version must be published once by hand (`cargo publish` in `rust/`). Then crate **Settings → Trusted Publishing**: repository `valueconnectinc/soritrading-watch-together`, workflow `release.yml`. |
| NuGet | `SoriTrading.WatchTogether` | **Trusted Publishing → Add policy**: repository owner `valueconnectinc`, repository `soritrading-watch-together`, workflow file `release.yml`. In GitHub, add the repository secret `NUGET_USER` = your nuget.org profile name (not the e-mail). |
| Go | `github.com/valueconnectinc/soritrading-watch-together/go` | Nothing — the Go module proxy reads tags. |

Before a hand-published first version, copy the root `LICENSE` into the package folder (the workflow does this itself).

## Each release

1. Set the same version in `js/package.json`, `python/pyproject.toml`, `rust/Cargo.toml` and `dotnet/WatchTogether/WatchTogether.csproj` (the workflow refuses a tag that does not match all four).
2. Commit, then tag and push both tags:

   ```sh
   git tag v0.1.0
   git tag go/v0.1.0      # Go reads tags with the module's folder as prefix
   git push origin v0.1.0 go/v0.1.0
   ```

3. Watch the `release` workflow. Each job runs the conformance vectors for its language before it publishes.
