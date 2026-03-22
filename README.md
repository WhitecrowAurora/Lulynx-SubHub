# Lulynx SubHub

Lulynx SubHub is a self-hosted proxy subscription hub for aggregating, refreshing, filtering, and exporting multiple subscription sources from one web panel.

It is designed for people who need to manage several remote subscriptions, local manual nodes, and multiple merged outputs for phones, routers, and different clients without relying on cron jobs or extra runtime dependencies.

## Highlights

- Self-hosted web panel with initial setup wizard and admin login
- Manage multiple remote subscriptions and local manual node lists
- Auto refresh with in-process countdown scheduler instead of cron
- Preview subscriptions before saving and inspect parsed nodes
- Group subscriptions, edit expiry dates, pause, delete, or bulk import
- Build multiple merge profiles with independent source selection and filters
- Prioritize selected sources so their nodes appear first in merged output
- Global and profile-level filtering for keywords and protocols
- Rename rules and dedup strategies for cleaner merged results
- Export merged outputs as Base64, plain text, JSON, Clash / Mihomo, Surge, and sing-box
- Access statistics, refresh logs, health alerts, Telegram / Webhook notifications
- Backup / restore, restore preview, install wizard, and schema migration support
- Stdlib-only Python backend with SQLite storage

## Supported Protocols

- `ss`
- `ssr`
- `vmess`
- `vless`
- `trojan`
- `hy2`
- `hysteria2`
- `anytls`

## Screens and Workflow

Lulynx SubHub is built around four core workflows:

1. Add remote subscriptions or local manual nodes.
2. Preview, refresh, filter, and organize them by group and expiry date.
3. Create one or more merge profiles for different devices or use cases.
4. Subscribe to the merged output URL in your client of choice.

The panel also tracks refresh history, node changes, health warnings, and merged subscription access counts.

## Project Structure

```text
app.py              HTTP server entrypoint
manager.py          SQLite storage, refresh scheduler, merge logic
parsers.py          Subscription decoding and node parsing
exporters.py        Output builders for Base64 / plain / JSON / Clash / Surge / sing-box
static/             Dashboard, setup, login, and frontend assets
examples/           Bulk import and filter rule examples
tests/              Unit tests
```

## Requirements

- Python 3.10+

No third-party runtime dependencies are required.

## Quick Start

```bash
python app.py --host 0.0.0.0 --port 8787 --db data/subpanel.db
```

Then open:

```text
http://127.0.0.1:8787
```

On first launch, the setup wizard lets you configure:

- admin username and password
- panel port
- default theme
- global exclude keywords
- excluded protocols
- dedup strategy
- rename rules
- cleanup strategy

## Main Features

### Subscription Sources

- Add remote subscription URLs
- Add local manual nodes, one URI per line
- Edit source URL, name, group, expiry date, and refresh interval
- Enable, disable, refresh, or delete sources
- Bulk import sources with `name,url` or `name<TAB>url`
- Preview subscriptions before saving

### Merge Profiles

- Create multiple merged outputs
- Select specific sources for each profile
- Use all enabled sources or only chosen ones
- Prioritize selected sources so they stay at the top of merged results
- Apply per-profile keyword and protocol exclusions
- Clone profiles and regenerate public tokens

### Filtering and Cleanup

- Global keyword filtering
- Global protocol exclusion
- Dedup by URI, name + protocol, or name
- Rename rules in `pattern => replacement` format
- Auto disable expired subscriptions
- Auto pause subscriptions after repeated failures

### Observability

- Dashboard stats and health alerts
- Refresh logs with status, duration, before/after counts, and sample diffs
- Node preview with search and protocol filters
- Access counters for merged subscription links
- Telegram and Webhook notifications

### Data Safety

- Full backup export
- Restore preview before replacing current data
- Database schema migration command
- Backend admin reset command for recovery

## Export Formats

Each merge profile exposes public URLs such as:

```text
/subscribe/<token>
/subscribe/<token>?format=plain
/subscribe/<token>?format=json
/subscribe/<token>?format=clash
/subscribe/<token>?format=surge
/subscribe/<token>?format=singbox
```

Available formats:

- `base64` for typical subscription clients
- `plain` for debugging raw merged URIs
- `json` for inspection or scripting
- `clash` for Clash / Mihomo proxy lists
- `surge` for Surge `[Proxy]` fragments
- `singbox` for sing-box `outbounds` JSON

## Reverse Proxy Notes

Lulynx SubHub is intended to run behind a reverse proxy on the domain root, for example:

- `https://sub.example.com/`

Subpath deployments such as `https://example.com/sub/` are not supported by the current frontend routing.

When using Nginx, keep `Host`, `X-Forwarded-Host`, and `X-Forwarded-Proto` headers so the panel can generate correct public subscription URLs.

## CLI Commands

Reset admin credentials:

```bash
python app.py --reset-admin
python app.py --reset-admin --admin-username admin --admin-password new-password-123
```

Run database migrations:

```bash
python app.py --migrate-db --db data/subpanel.db
```

## Development

Run tests:

```bash
python -m unittest discover -s tests -v
```

The repository includes example files under [`examples/`](./examples) for:

- bulk import
- global filters
- rename rules

## Security and Publishing Notes

- Do not commit `data/` or any generated SQLite database
- Do not publish live subscription URLs, tokens, webhook URLs, or notification credentials
- Backups intentionally exclude the runtime `session_secret`

## License

This project is licensed under the GNU Affero General Public License v3.0.
