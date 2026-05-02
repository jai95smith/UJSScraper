# UJS / GavelSearch — Full Stack Restore

Cold-start runbook to bring back the entire UJS + GavelSearch stack after the 2026-05-02 decommission.

## What was decommissioned

- Droplet `ldnt-ujs` (134.209.117.83, s-1vcpu-2gb, nyc1) — hosted UJS API, UJS worker, UJS web UI **and the live `gavelsearch.com` + `api.gavelsearch.com` site (nginx + SSL termination)**
- Managed Postgres cluster `ldnt-ujs-db` (db-s-1vcpu-1gb pg17, nyc1) — `ujs` DB ~247 MB, ~470K rows
- 8× `ujs-proxy-*` droplets — already gone (see `docs/proxies.md`)

## Backup location

Local laptop: **`~/Backups/do-decommission-2026-05-02/ujs/`**

| File | Purpose |
|---|---|
| `ujs-2026-05-02.dump` | UJS DB pg_dump (custom format, 22 MB compressed). **Round-trip verified — 25/25 tables exact COUNT(*) match.** sha256: `a7fb945c...` |
| `ujs-cluster-defaultdb-2026-05-02.dump` | `defaultdb` (admin DB, ~empty) |
| `ujs-rowcounts-pre.txt` / `-restored.txt` | Verification artifacts |
| `analyses_backup_20260409.json` | Pre-existing 14 MB Gemini analysis snapshot (Apr 9) |
| `artifacts/ujs.env` | Production env (DB URLs, API keys: Gemini, Anthropic, Brevo, etc.) |
| `artifacts/ujs.env.bak.20260502` | Same with `UJS_PROXIES` line still present (in case useful) |
| `artifacts/ujs-api.service` | systemd unit — `ujs api --port 8100` |
| `artifacts/ujs-web.service` | systemd unit — Flask web UI on :8000 (used by gavelsearch.com) |
| `artifacts/ujs-worker.service` | systemd unit — background worker |
| `artifacts/root-crontab.txt` | Root crontab (currently with HALTED entries) |
| `artifacts/nginx-sites-available/gavelsearch` | Nginx vhost for `gavelsearch.com`, `www.gavelsearch.com`, `api.gavelsearch.com` |
| `artifacts/nginx-sites-available/default` | Default nginx site |
| `artifacts/ssl/gavelsearch.crt` | TLS cert (10-year self-signed, valid through 2036-04-05 — likely Cloudflare Origin Cert) |
| `artifacts/ssl/gavelsearch.key` | TLS private key (chmod 600) |

Code: `git@github.com:jai95smith/UJSScraper.git`

## Restore procedure

### 1. New managed Postgres cluster

```bash
doctl databases create ldnt-ujs-db \
  --context lehighdaily \
  --engine pg --version 17 \
  --size db-s-1vcpu-1gb --region nyc1 --num-nodes 1
# Wait for "online" status
doctl databases list --context lehighdaily
```

Grab the connection URI:
```bash
doctl databases connection <new-cluster-id> --context lehighdaily --format URI
```

### 2. Create the `ujs` database, restore dump

```bash
HOST=<new-host-from-above>
PASS=<new-doadmin-password>
export PGPASSWORD=$PASS
psql "postgres://doadmin@$HOST:25060/defaultdb?sslmode=require" -c "CREATE DATABASE ujs;"
pg_restore --no-owner --no-acl -h $HOST -p 25060 -U doadmin -d ujs \
  ~/Backups/do-decommission-2026-05-02/ujs/ujs-2026-05-02.dump
```

Verify row counts match `ujs-rowcounts-restored.txt`.

### 3. New droplet

```bash
doctl compute droplet create ldnt-ujs \
  --context lehighdaily \
  --region nyc1 \
  --size s-1vcpu-2gb \
  --image ubuntu-24-04-x64 \
  --ssh-keys "$(doctl compute ssh-key list --context lehighdaily --format ID --no-header | head -1)" \
  --wait
NEW_IP=$(doctl compute droplet list ldnt-ujs --context lehighdaily --format PublicIPv4 --no-header)
```

Add the new droplet to the DB cluster's trusted sources:
```bash
doctl databases firewalls append <new-cluster-id> --context lehighdaily \
  --rule droplet:$(doctl compute droplet list ldnt-ujs --context lehighdaily --format ID --no-header)
```

### 4. Provision the droplet

```bash
ssh -i ~/.ssh/id_personal root@$NEW_IP bash <<'EOF'
apt-get update -qq
apt-get install -y python3.12 python3.12-venv python3-pip nginx
mkdir -p /opt/ujs/logs /opt/ujs/pdfs
EOF
```

### 5. Push code + env + systemd

```bash
# Code
rsync -avz --exclude '.git' --exclude '__pycache__' --exclude '.venv' --exclude 'pdfs' --exclude 'logs/*.log' \
  -e "ssh -i ~/.ssh/id_personal" \
  /Users/jai/VSCodeProjects/UJSScraper/ root@$NEW_IP:/opt/ujs/

# Env (UPDATE DATABASE_URL with new host + password before scp)
scp -i ~/.ssh/id_personal ~/Backups/do-decommission-2026-05-02/ujs/artifacts/ujs.env root@$NEW_IP:/opt/ujs/.env

# Build venv
ssh -i ~/.ssh/id_personal root@$NEW_IP "cd /opt/ujs && python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt"

# Systemd units
scp -i ~/.ssh/id_personal ~/Backups/do-decommission-2026-05-02/ujs/artifacts/ujs-*.service root@$NEW_IP:/etc/systemd/system/
ssh -i ~/.ssh/id_personal root@$NEW_IP "systemctl daemon-reload && systemctl enable --now ujs-api ujs-web"
```

### 6. Restore nginx + SSL for GavelSearch

```bash
scp -i ~/.ssh/id_personal ~/Backups/do-decommission-2026-05-02/ujs/artifacts/ssl/gavelsearch.crt root@$NEW_IP:/etc/ssl/certs/
scp -i ~/.ssh/id_personal ~/Backups/do-decommission-2026-05-02/ujs/artifacts/ssl/gavelsearch.key root@$NEW_IP:/etc/ssl/private/
ssh -i ~/.ssh/id_personal root@$NEW_IP "chmod 600 /etc/ssl/private/gavelsearch.key"

scp -i ~/.ssh/id_personal ~/Backups/do-decommission-2026-05-02/ujs/artifacts/nginx-sites-available/gavelsearch root@$NEW_IP:/etc/nginx/sites-available/
ssh -i ~/.ssh/id_personal root@$NEW_IP "ln -sf /etc/nginx/sites-available/gavelsearch /etc/nginx/sites-enabled/ && nginx -t && systemctl reload nginx"
```

### 7. Update DNS + Cloudflare

In Cloudflare (zone `gavelsearch.com`, scoped DNS token in 1Password / `reference_cloudflare_api.md`):
- `gavelsearch.com` A → `$NEW_IP`
- `www.gavelsearch.com` A → `$NEW_IP`
- `api.gavelsearch.com` A → `$NEW_IP`
- Proxy status: orange cloud (proxied). The 10-year origin cert on the box is valid through 2036, so no Let's Encrypt needed.

### 8. Re-enable cron (optional)

The crontab in `artifacts/root-crontab.txt` has two HALTED entries. Uncomment if you want hourly watchdog + daily Brevo notifications:

```bash
ssh -i ~/.ssh/id_personal root@$NEW_IP "crontab -e"
# Remove the '# HALTED:' prefix on both lines
```

### 9. Smoke test

```bash
curl -s https://api.gavelsearch.com/healthz
curl -s https://gavelsearch.com/ | head -20
ssh -i ~/.ssh/id_personal root@$NEW_IP "tail -10 /opt/ujs/logs/ingest.log"
```

## Reference data captured at decommission

- DB row counts (live):

```
docket_entries     146128
participants        84582
cases               80734
charges             42609
api_costs           31919
analyses            31023
attorneys           15896
events               7792
system_log           7746
ingest_queue         6795
bail                 4688
change_log           4533
scrape_log           4330
charge_embeddings    1654
sentences            1623
query_log             235
chat_jobs             162
conversations          32
user_watches            5
user_preferences        2
app_settings            2
```

- Original Cloudflare DNS A records pointed to `134.209.117.83`.
- Brevo, Gemini, Anthropic, OpenAI API keys all stored in `ujs.env`. Rotate if env file ever leaks.

## Cost reference

Steady-state was ~$28/mo:
- Droplet `ldnt-ujs` (s-1vcpu-2gb): ~$12
- DB cluster `ldnt-ujs-db`: ~$13.93 + ~$2.30 storage = ~$16.23
