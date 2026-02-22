#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# seed.sh — Seeds OxideDB with test buckets and sample data
# Usage:
#   ./scripts/seed.sh              (default: localhost:8091)
#   ./scripts/seed.sh 8092         (custom port)
# ─────────────────────────────────────────────────────────────
set -euo pipefail

PORT="${1:-8091}"
BASE="http://127.0.0.1:${PORT}"
API="${BASE}/api/v1"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}✅ $*${NC}"; }
info() { echo -e "${CYAN}ℹ️  $*${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $*${NC}"; }

# ── Health check ─────────────────────────────────────────────
info "Checking server at: ${BASE}"
if ! curl -sf "${BASE}/pools" > /dev/null 2>&1; then
    echo "❌ Server is not running at ${BASE}!"
    echo ""
    echo "Start the server first:"
    echo "  make dev      # run in the background"
    echo "  make run      # run in the foreground"
    exit 1
fi
log "Server is running"

# ═══════════════════════════════════════════════════════════════
# 1) BUCKETS
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 📦 Creating buckets..."
echo "═══════════════════════════════════════════════════════════"

# travel-sample bucket
curl -sf -X POST "${API}/buckets" \
  -H 'Content-Type: application/json' \
  -d '{"name":"travel-sample","bucket_type":"couchbase","ram_quota_mb":256,"num_replicas":0}' \
  > /dev/null 2>&1 && log "travel-sample bucket created" || warn "travel-sample already exists"

# test-bucket
curl -sf -X POST "${API}/buckets" \
  -H 'Content-Type: application/json' \
  -d '{"name":"test-bucket","bucket_type":"couchbase","ram_quota_mb":128,"num_replicas":0}' \
  > /dev/null 2>&1 && log "test-bucket created" || warn "test-bucket already exists"

# users bucket
curl -sf -X POST "${API}/buckets" \
  -H 'Content-Type: application/json' \
  -d '{"name":"users","bucket_type":"couchbase","ram_quota_mb":128,"num_replicas":0}' \
  > /dev/null 2>&1 && log "users bucket created" || warn "users already exists"

# ═══════════════════════════════════════════════════════════════
# 2) TRAVEL-SAMPLE DATA
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " ✈️  Loading travel-sample data..."
echo "═══════════════════════════════════════════════════════════"

DOC_BASE="${API}/docs/travel-sample/scopes/_default/collections/_default/docs"

# Airlines
for i in 1 2 3 4 5; do
  curl -sf -X PUT "${DOC_BASE}/airline_${i}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"value\": {
        \"type\": \"airline\",
        \"id\": ${i},
        \"name\": \"Airline ${i}\",
        \"iata\": \"A${i}\",
        \"icao\": \"AIR${i}\",
        \"callsign\": \"AIRLINE${i}\",
        \"country\": \"$([ $((i % 2)) -eq 0 ] && echo 'Turkey' || echo 'United States')\"
      }
    }" > /dev/null 2>&1
done
log "5 airline documents inserted"

# Airports
airports=(
  '{"type":"airport","id":1,"airportname":"Istanbul Airport","city":"Istanbul","country":"Turkey","faa":"IST","icao":"LTFM","tz":"Europe/Istanbul","geo":{"lat":41.2753,"lon":28.7519}}'
  '{"type":"airport","id":2,"airportname":"Sabiha Gokcen","city":"Istanbul","country":"Turkey","faa":"SAW","icao":"LTFJ","tz":"Europe/Istanbul","geo":{"lat":40.8986,"lon":29.3092}}'
  '{"type":"airport","id":3,"airportname":"JFK International","city":"New York","country":"United States","faa":"JFK","icao":"KJFK","tz":"America/New_York","geo":{"lat":40.6413,"lon":-73.7781}}'
  '{"type":"airport","id":4,"airportname":"Heathrow","city":"London","country":"United Kingdom","faa":"LHR","icao":"EGLL","tz":"Europe/London","geo":{"lat":51.4700,"lon":-0.4543}}'
  '{"type":"airport","id":5,"airportname":"Esenboga","city":"Ankara","country":"Turkey","faa":"ESB","icao":"LTAC","tz":"Europe/Istanbul","geo":{"lat":40.1281,"lon":32.9951}}'
)
for i in "${!airports[@]}"; do
  idx=$((i + 1))
  curl -sf -X PUT "${DOC_BASE}/airport_${idx}" \
    -H 'Content-Type: application/json' \
    -d "{\"value\": ${airports[$i]}}" > /dev/null 2>&1
done
log "5 airport documents inserted"

# Routes
for i in 1 2 3 4 5; do
  src=$((RANDOM % 5 + 1))
  dst=$(( (src % 5) + 1 ))
  curl -sf -X PUT "${DOC_BASE}/route_${i}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"value\": {
        \"type\": \"route\",
        \"id\": ${i},
        \"airline\": \"A${i}\",
        \"sourceairport\": \"airport_${src}\",
        \"destinationairport\": \"airport_${dst}\",
        \"distance\": $((RANDOM % 5000 + 500)),
        \"stops\": $((RANDOM % 2)),
        \"equipment\": \"$([ $((i % 2)) -eq 0 ] && echo '777' || echo 'A320')\"
      }
    }" > /dev/null 2>&1
done
log "5 route documents inserted"

# Hotels
hotels=(
  '{"type":"hotel","id":1,"name":"Grand Istanbul Hotel","city":"Istanbul","country":"Turkey","address":"Taksim Square 1","price":150,"rating":4.5,"pets_ok":true,"free_parking":true}'
  '{"type":"hotel","id":2,"name":"Bosphorus Palace","city":"Istanbul","country":"Turkey","address":"Ortakoy Marina 5","price":280,"rating":4.8,"pets_ok":false,"free_parking":false}'
  '{"type":"hotel","id":3,"name":"Manhattan Suites","city":"New York","country":"United States","address":"5th Avenue 100","price":350,"rating":4.2,"pets_ok":true,"free_parking":false}'
  '{"type":"hotel","id":4,"name":"London Bridge Inn","city":"London","country":"United Kingdom","address":"Tower Hill 22","price":200,"rating":4.0,"pets_ok":false,"free_parking":true}'
  '{"type":"hotel","id":5,"name":"Ankara Residence","city":"Ankara","country":"Turkey","address":"Kizilay Blvd 15","price":90,"rating":3.8,"pets_ok":true,"free_parking":true}'
)
for i in "${!hotels[@]}"; do
  idx=$((i + 1))
  curl -sf -X PUT "${DOC_BASE}/hotel_${idx}" \
    -H 'Content-Type: application/json' \
    -d "{\"value\": ${hotels[$i]}}" > /dev/null 2>&1
done
log "5 hotel documents inserted"

# ═══════════════════════════════════════════════════════════════
# 3) USERS BUCKET DATA
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 👤 Loading users data..."
echo "═══════════════════════════════════════════════════════════"

USER_BASE="${API}/docs/users/scopes/_default/collections/_default/docs"

users=(
  '{"type":"user","username":"anil","email":"anil@example.com","name":"Anil Yilmaz","role":"admin","active":true,"created":"2025-01-15T10:30:00Z"}'
  '{"type":"user","username":"mehmet","email":"mehmet@example.com","name":"Mehmet Demir","role":"developer","active":true,"created":"2025-02-20T14:00:00Z"}'
  '{"type":"user","username":"ayse","email":"ayse@example.com","name":"Ayse Kaya","role":"analyst","active":true,"created":"2025-03-10T09:15:00Z"}'
  '{"type":"user","username":"john","email":"john@example.com","name":"John Smith","role":"viewer","active":false,"created":"2024-12-01T08:00:00Z"}'
  '{"type":"user","username":"elena","email":"elena@example.com","name":"Elena Popov","role":"developer","active":true,"created":"2025-04-05T16:45:00Z"}'
)
for i in "${!users[@]}"; do
  uname=$(echo "${users[$i]}" | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
  curl -sf -X PUT "${USER_BASE}/user::${uname}" \
    -H 'Content-Type: application/json' \
    -d "{\"value\": ${users[$i]}}" > /dev/null 2>&1
done
log "5 user documents inserted"

# ═══════════════════════════════════════════════════════════════
# 4) TEST-BUCKET DATA
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 🧪 Loading test-bucket data..."
echo "═══════════════════════════════════════════════════════════"

TEST_BASE="${API}/docs/test-bucket/scopes/_default/collections/_default/docs"

for i in $(seq 1 10); do
  curl -sf -X PUT "${TEST_BASE}/doc_${i}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"value\": {
        \"type\": \"test\",
        \"id\": ${i},
        \"name\": \"Test Document ${i}\",
        \"score\": $((RANDOM % 100)),
        \"active\": $([ $((i % 3)) -eq 0 ] && echo 'false' || echo 'true'),
        \"tags\": [\"tag-${i}\", \"test\", \"sample\"],
        \"nested\": {
          \"field1\": \"value-${i}\",
          \"field2\": $((i * 10))
        }
      }
    }" > /dev/null 2>&1
done
log "10 test documents inserted"

# ═══════════════════════════════════════════════════════════════
# 5) INDEXES
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 🗂️  Creating indexes..."
echo "═══════════════════════════════════════════════════════════"

QUERY="${BASE}/query/service"

curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"CREATE INDEX idx_airline_country ON `travel-sample`(country) WHERE type=\"airline\""}' \
  > /dev/null 2>&1 && log "idx_airline_country created" || warn "index already exists"

curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"CREATE INDEX idx_airport_city ON `travel-sample`(city)"}' \
  > /dev/null 2>&1 && log "idx_airport_city created" || warn "index already exists"

curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"CREATE INDEX idx_hotel_country ON `travel-sample`(country) WHERE type=\"hotel\""}' \
  > /dev/null 2>&1 && log "idx_hotel_country created" || warn "index already exists"

curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"CREATE INDEX idx_user_role ON `users`(role)"}' \
  > /dev/null 2>&1 && log "idx_user_role created" || warn "index already exists"

# ═══════════════════════════════════════════════════════════════
# 6) VERIFICATION
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 🔍 Running verification queries..."
echo "═══════════════════════════════════════════════════════════"

echo ""
info "SELECT * FROM travel-sample WHERE type='airline'"
curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT * FROM `travel-sample` WHERE type = \"airline\""}' \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('results', [])
print(f'  → {len(results)} airlines found')
" 2>/dev/null || warn "query failed"

info "SELECT * FROM travel-sample WHERE type='hotel' AND country='Turkey'"
curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT * FROM `travel-sample` WHERE type = \"hotel\" AND country = \"Turkey\""}' \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('results', [])
print(f'  → {len(results)} hotels in Turkey found')
" 2>/dev/null || warn "query failed"

info "SELECT * FROM users WHERE role='developer'"
curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT * FROM `users` WHERE role = \"developer\""}' \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('results', [])
print(f'  → {len(results)} developers found')
" 2>/dev/null || warn "query failed"

info "SELECT 'keep alive' (DataGrip ping test)"
curl -sf -X POST "${QUERY}" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT '\''keep alive'\''"}' \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('results', [])
if results:
    print(f'  → Success: {results[0]}')
else:
    print('  → No results')
" 2>/dev/null || warn "query failed"

# ═══════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " 🎉 SEED COMPLETE!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo " Buckets:"
echo "   • travel-sample  → 20 documents (airline, airport, route, hotel)"
echo "   • users          →  5 documents (user profiles)"
echo "   • test-bucket    → 10 documents (test data)"
echo ""
echo " Connection details:"
echo "   REST API   : ${BASE}"
echo "   Query (N1QL): ${QUERY}"
echo "   Memcached  : 127.0.0.1:11210"
echo ""
echo " DataGrip connection:"
echo "   Host     : 127.0.0.1"
echo "   Port     : ${PORT}"
echo "   Username : Administrator"
echo "   Password : password"
echo "   Bucket   : travel-sample"
echo ""
echo " Example queries:"
echo "   SELECT * FROM \`travel-sample\` WHERE type = 'airline'"
echo "   SELECT * FROM \`travel-sample\` WHERE city = 'Istanbul'"
echo "   SELECT * FROM \`users\` WHERE role = 'developer'"
echo "   SELECT COUNT(*) FROM \`test-bucket\` WHERE active = true"
echo ""
