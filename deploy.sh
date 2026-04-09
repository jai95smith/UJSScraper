#!/bin/bash
# Deploy UJSScraper: push to GitHub + rsync to droplet + rolling restart
# NOTE: ujs-worker is NOT restarted — it runs independently to avoid killing in-progress analysis
set -e

git push origin main &
rsync -avz --exclude '.git' --exclude '__pycache__' --exclude '.venv' --exclude 'pdfs' --exclude 'logs/*.log' -e "ssh -i ~/.ssh/id_personal" /Users/jai/VSCodeProjects/UJSScraper/ root@134.209.117.83:/opt/ujs/ &
wait

# Restart web + API only (worker stays running)
ssh -i ~/.ssh/id_personal root@134.209.117.83 'systemctl restart ujs-web && sleep 1 && systemctl restart ujs-api'
echo "Deployed and restarted (worker untouched)."
