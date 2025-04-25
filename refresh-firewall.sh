#!/usr/bin/env bash

# ── CONFIGURE THESE ─────────────────────────────────────
RESOURCE_GROUP="RetailResourceGroup"
SERVER_NAME="retailsqlsrv29"
RULE_NAME="DevClient"
# ────────────────────────────────────────────────────────

# 1️⃣ Get your current public IPv4
CURRENT_IP=$(curl -s4 https://ifconfig.co)
echo "Current public IP is $CURRENT_IP"

# 2️⃣ Delete old rule if it exists
if az sql server firewall-rule show \
     --resource-group "$RESOURCE_GROUP" \
     --server "$SERVER_NAME" \
     --name "$RULE_NAME" &>/dev/null; then
  echo "Deleting old rule '$RULE_NAME'..."
  az sql server firewall-rule delete \
    --resource-group "$RESOURCE_GROUP" \
    --server "$SERVER_NAME" \
    --name "$RULE_NAME"
fi

# 3️⃣ Create a new rule for your IP
echo "Creating firewall rule '$RULE_NAME' for $CURRENT_IP..."
az sql server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --server "$SERVER_NAME" \
  --name "$RULE_NAME" \
  --start-ip-address "$CURRENT_IP" \
  --end-ip-address   "$CURRENT_IP"

echo "✅ Firewall updated. Azure SQL now allows $CURRENT_IP"
