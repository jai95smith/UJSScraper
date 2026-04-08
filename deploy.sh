#!/bin/bash
# Deploy UJSScraper: push to GitHub + rsync to droplet + restart API
set -e

git push origin main &
rsync -avz --exclude '.git' --exclude '__pycache__' --exclude '.venv' --exclude 'pdfs' --exclude 'logs/*.log' -e "ssh -i ~/.ssh/id_personal" /Users/jai/VSCodeProjects/UJSScraper/ root@134.209.117.83:/opt/ujs/ &
wait

ssh -i ~/.ssh/id_personal root@134.209.117.83 'systemctl restart ujs-api'
echo "Deployed and restarted."
