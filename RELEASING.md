# Releasing

All packages share one version. Publishing runs in GitHub Actions (`.github/workflows/release.yml`) with **trusted publishing** — the registries trust this repository's workflow through GitHub OIDC, so no registry token is stored here. The exception is Maven Central, which has no trusted publishing and uses a token and a GPG key kept as repository secrets.

## One-time setup

| registry | package | what to configure |
|---|---|---|
| npm | `soritrading-watch-together` | The first version must be published once by hand (`npm publish` in `js/`, logged in with 2FA). Then: package **Settings → Trusted publishing → GitHub Actions**: owner `valueconnectinc`, repository `soritrading-watch-together`, workflow `release.yml`, allow **npm publish**. A new configuration must be used within 2 days. |
| PyPI | `soritrading-watch-together` | **Publishing → Add a pending publisher** (works before the project exists): owner `valueconnectinc`, repository `soritrading-watch-together`, workflow `release.yml`, environment empty. |
| crates.io | `soritrading-watch-together` | The first version must be published once by hand (`cargo publish` in `rust/`). Then crate **Settings → Trusted Publishing**: repository `valueconnectinc/soritrading-watch-together`, workflow `release.yml`. |
| NuGet | `SoriTrading.WatchTogether` | **Trusted Publishing → Add policy**: repository owner `valueconnectinc`, repository `soritrading-watch-together`, workflow file `release.yml`. In GitHub, add the repository secret `NUGET_USER` = your nuget.org profile name (not the e-mail). |
| Maven Central | `io.github.valueconnectinc:soritrading-watch-together` | No trusted publishing here, so this one uses secrets. At [central.sonatype.com](https://central.sonatype.com): **Register New Namespace** `io.github.valueconnectinc` and prove ownership by creating an empty public repository named after the verification key under the `valueconnectinc` GitHub account (delete it once verified; signing in with that GitHub account verifies it directly). Then **View Account → Generate User Token**. Create a GPG key and send the public key to `keys.openpgp.org` or `keyserver.ubuntu.com`. In GitHub, add the repository secrets `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` (the token's two halves), `MAVEN_GPG_KEY` (the ASCII-armored private key) and `MAVEN_GPG_PASSPHRASE`. Published versions can never be removed or changed. |
| Go | `github.com/valueconnectinc/soritrading-watch-together/go` | Nothing — the Go module proxy reads tags. |

Before a hand-published first version, copy the root `LICENSE` into the package folder (the workflow does this itself).

A version published by hand cannot be published again, so the first tag after a hand publish must carry the **next** version — otherwise that registry's job fails. This setup was done on 2026-10-22: npm and crates.io have 0.1.0 by hand, and every registry got 0.1.1 from the workflow.

## Each release

1. Set the same version in `js/package.json`, `python/pyproject.toml`, `rust/Cargo.toml`, `dotnet/WatchTogether/WatchTogether.csproj` and `jvm/pom.xml` (the project's own `<version>`, not a plugin's; the workflow refuses a tag that does not match all five).
2. Commit, then tag and push both tags:

   ```sh
   git tag v0.1.2
   git tag go/v0.1.2      # Go reads tags with the module's folder as prefix
   git push origin v0.1.2 go/v0.1.2
   ```

3. Watch the `release` workflow. Each job runs the conformance vectors for its language before it publishes.
