#!/bin/bash
# Deploy UJSScraper: push to GitHub + rsync to droplet + rolling restart
set -e

git push origin main &
rsync -avz --exclude '.git' --exclude '__pycache__' --exclude '.venv' --exclude 'pdfs' --exclude 'logs/*.log' -e "ssh -i ~/.ssh/id_personal" /Users/jai/VSCodeProjects/UJSScraper/ root@134.209.117.83:/opt/ujs/ &
wait

# Rolling restart — one service at a time so the other stays up
ssh -i ~/.ssh/id_personal root@134.209.117.83 'systemctl restart ujs-web && sleep 1 && systemctl restart ujs-api'
echo "Deployed and restarted."
