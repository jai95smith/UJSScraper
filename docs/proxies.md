# UJS Proxy Pool — Recreate from Scratch

The scraper can route outbound requests through N HTTP proxies (IP rotation against PA UJS portal rate limits). The proxy IPs go into `UJS_PROXIES` in `/opt/ujs/.env` on the `ldnt-ujs` droplet (`134.209.117.83`).

The pool was destroyed on 2026-05-02 because it had been idle for ~3 weeks. This doc is the runbook to bring it back.

## Status when destroyed

- 8× DigitalOcean droplets, all `s-1vcpu-512mb-10gb` in `nyc1` (~$4/mo each, ~$32/mo total)
- All ran `tinyproxy` only (Ubuntu package `tinyproxy 1.11.1-3ubuntu0.1`)
- Identical config on every box (see below)
- DO context: `lehighdaily` (account 34189501)

## Recreate the pool

```bash
# 1. Spin up 8 droplets (adjust count as needed)
for i in 1 2 3 4 5 6 7 8; do
  doctl compute droplet create "ujs-proxy-$i" \
    --context lehighdaily \
    --region nyc1 \
    --size s-1vcpu-512mb-10gb \
    --image ubuntu-24-04-x64 \
    --ssh-keys "$(doctl compute ssh-key list --context lehighdaily --format ID --no-header | head -1)" \
    --wait
done

# 2. Collect their public IPs
doctl compute droplet list --context lehighdaily --format Name,PublicIPv4 | grep ujs-proxy

# 3. Install + configure tinyproxy on each (replace IPs below)
PROXY_IPS="IP1 IP2 IP3 IP4 IP5 IP6 IP7 IP8"
for ip in $PROXY_IPS; do
  ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_personal root@$ip bash <<'EOF'
apt-get update -qq && apt-get install -y -qq tinyproxy
cat > /etc/tinyproxy/tinyproxy.conf <<CFG
Port 8888
Listen 0.0.0.0
Timeout 600
Allow 134.209.117.83
Allow 127.0.0.1
MaxClients 20
LogLevel Error
CFG
systemctl restart tinyproxy
systemctl enable tinyproxy
EOF
done

# 4. Quick health check
for ip in $PROXY_IPS; do
  echo -n "$ip: "
  curl -s -x http://$ip:8888 -o /dev/null -w "%{http_code}\n" --max-time 5 https://ujsportal.pacourts.us/
done
```

## Wire the pool back into UJS

```bash
ssh -i ~/.ssh/id_personal root@134.209.117.83
# Edit /opt/ujs/.env — uncomment / set:
UJS_PROXIES=IP1:8888,IP2:8888,IP3:8888,IP4:8888,IP5:8888,IP6:8888,IP7:8888,IP8:8888
systemctl restart ujs-api
```

Code path: `ujs/core.py:53` and `ujs/api.py:31` both read `UJS_PROXIES` env (comma-separated `IP:PORT`).

## Tinyproxy config (canonical)

```
Port 8888
Listen 0.0.0.0
Timeout 600
Allow 134.209.117.83
Allow 127.0.0.1
MaxClients 20
LogLevel Error
```

`Allow` only permits the UJS API droplet (`134.209.117.83`) + localhost — proxies are not open to the internet. If the UJS droplet IP changes, update the `Allow` line.

## How the IPs were last allocated (for reference)

Destroyed 2026-05-02. Names + IPs at the time:

| Name | IP |
|---|---|
| ujs-proxy-1 | 198.211.116.222 |
| ujs-proxy-2 | 134.209.208.189 |
| ujs-proxy-3 | 206.189.191.209 |
| ujs-proxy-4 | 159.223.183.248 |
| ujs-proxy-5 | 68.183.23.25 |
| ujs-proxy-6 | 134.209.209.240 |
| ujs-proxy-7 | 157.245.128.181 |
| ujs-proxy-8 | 159.223.174.12 |

New droplets get fresh IPs — always re-collect via `doctl` after creation.
