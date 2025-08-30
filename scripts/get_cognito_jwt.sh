#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/get_cognito_jwt.sh <REGION> <USER_POOL_ID> <CLIENT_ID> <USERNAME> <PASSWORD>
if [ $# -lt 5 ]; then
  echo "Usage: $0 <REGION> <USER_POOL_ID> <CLIENT_ID> <USERNAME> <PASSWORD>" >&2
  exit 1
fi

REGION="$1"; USER_POOL_ID="$2"; CLIENT_ID="$3"; USERNAME="$4"; PASSWORD="$5"

get_token() {
  local field="$1"
  # Try USER_PASSWORD_AUTH first, then ADMIN_USER_PASSWORD_AUTH
  aws cognito-idp initiate-auth \
    --region "$REGION" \
    --auth-flow USER_PASSWORD_AUTH \
    --client-id "$CLIENT_ID" \
    --auth-parameters USERNAME="$USERNAME",PASSWORD="$PASSWORD" \
    --query "AuthenticationResult.${field}" \
    --output text 2>/dev/null \
  || aws cognito-idp admin-initiate-auth \
    --region "$REGION" \
    --user-pool-id "$USER_POOL_ID" \
    --client-id "$CLIENT_ID" \
    --auth-flow ADMIN_USER_PASSWORD_AUTH \
    --auth-parameters USERNAME="$USERNAME",PASSWORD="$PASSWORD" \
    --query "AuthenticationResult.${field}" \
    --output text 2>/dev/null \
  || true
}

ID_TOKEN="$(get_token IdToken)"
ACCESS_TOKEN="$(get_token AccessToken)"
REFRESH_TOKEN="$(get_token RefreshToken)"

if [ -z "$ID_TOKEN" ] || [ -z "$ACCESS_TOKEN" ]; then
  echo "ERROR: Auth failed. Check username/password and that the app client allows USER_PASSWORD_AUTH (and/or ADMIN_USER_PASSWORD_AUTH)." >&2
  aws cognito-idp describe-user-pool-client \
    --region "$REGION" \
    --user-pool-id "$USER_POOL_ID" \
    --client-id "$CLIENT_ID" \
    --query 'UserPoolClient.ExplicitAuthFlows' \
    --output json >&2 || true
  exit 2
fi

# Safely export (properly quoted for eval)
printf 'export ID_TOKEN=%q\n' "$ID_TOKEN"
printf 'export ACCESS_TOKEN=%q\n' "$ACCESS_TOKEN"
[ -n "$REFRESH_TOKEN" ] && printf 'export REFRESH_TOKEN=%q\n' "$REFRESH_TOKEN"
