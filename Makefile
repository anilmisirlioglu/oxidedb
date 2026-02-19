.PHONY: build run dev clean stop restart test logs help \
       cluster cluster-stop cluster-status cluster-down cluster-up \
       node1 node2 node3 stop1 stop2 stop3 \
       join join1 join2 join3 rebalance \
       down up kill-all

# ── Config ──────────────────────────────────────────────────────
BINARY      := oxidedb
CARGO       := cargo
DATA_DIR    := /tmp/oxidedb-sdk-test
PORT        := 8091
MEMC_PORT   := 11210
NODE_NAME   := node-1
PID_FILE    := .server.pid
LOG_FILE    := .server.log

# Multi-node config
NUM_NODES   ?= 3

# Persistence: set to false to disable WAL persistence
PERSIST     ?= true

# Extra args (override: make run EXTRA="--tls-enabled")
EXTRA       ?=

# ── Internal: build persistence flag ────────────────────────────
ifeq ($(PERSIST),false)
  _PERSIST_FLAG := --enable-persistence false
else
  _PERSIST_FLAG :=
endif

# ── Build ───────────────────────────────────────────────────────
build:  ## Build in debug mode
	$(CARGO) build

release:  ## Build in release mode
	$(CARGO) build --release

# ── Run ─────────────────────────────────────────────────────────
run: build  ## Build and run in foreground
	RUST_LOG=info $(CARGO) run -- \
		--port $(PORT) \
		--memcached-port $(MEMC_PORT) \
		--node-name $(NODE_NAME) \
		--data-dir $(DATA_DIR) \
		$(_PERSIST_FLAG) \
		$(EXTRA)

dev: build  ## Build and run in background (logs → .server.log)
	@$(MAKE) stop 2>/dev/null || true
	@echo "Starting $(BINARY) in background..."
	RUST_LOG=info ./target/debug/$(BINARY) \
		--port $(PORT) \
		--memcached-port $(MEMC_PORT) \
		--node-name $(NODE_NAME) \
		--data-dir $(DATA_DIR) \
		$(_PERSIST_FLAG) \
		$(EXTRA) \
		> $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@sleep 1
	@if kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "✅ Server running (pid=$$(cat $(PID_FILE))) on http://0.0.0.0:$(PORT)"; \
	else \
		echo "❌ Server failed to start. Check $(LOG_FILE)"; \
		rm -f $(PID_FILE); \
		exit 1; \
	fi

# ── Stop ────────────────────────────────────────────────────────
stop:  ## Stop the background server
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "Stopping server (pid=$$PID)..."; \
			kill $$PID; \
			sleep 1; \
			kill -0 $$PID 2>/dev/null && kill -9 $$PID; \
		fi; \
		rm -f $(PID_FILE); \
		echo "Server stopped."; \
	else \
		echo "No PID file found. Trying pkill..."; \
		pkill -f "target/debug/$(BINARY)" || true; \
	fi

# ── Restart ─────────────────────────────────────────────────────
restart: stop dev  ## Rebuild, stop old, start new (background)

# ── Quick restart (skip build if binary exists) ─────────────────
quick: stop  ## Stop and restart WITHOUT rebuilding
	@echo "Quick-starting $(BINARY)..."
	RUST_LOG=info ./target/debug/$(BINARY) \
		--port $(PORT) \
		--memcached-port $(MEMC_PORT) \
		--node-name $(NODE_NAME) \
		--data-dir $(DATA_DIR) \
		$(_PERSIST_FLAG) \
		$(EXTRA) \
		> $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@sleep 1
	@if kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "✅ Server running (pid=$$(cat $(PID_FILE))) on http://0.0.0.0:$(PORT)"; \
	else \
		echo "❌ Server failed to start. Check $(LOG_FILE)"; \
		rm -f $(PID_FILE); \
		exit 1; \
	fi

# ── Logs ────────────────────────────────────────────────────────
logs:  ## Tail the background server logs
	@tail -f $(LOG_FILE)

# ── Clean ───────────────────────────────────────────────────────
clean: stop  ## Stop server, remove build artifacts and data
	$(CARGO) clean
	rm -rf $(DATA_DIR) $(PID_FILE) $(LOG_FILE)

clean-data:  ## Remove only data dir (fresh DB on next start)
	@$(MAKE) stop 2>/dev/null || true
	rm -rf $(DATA_DIR)
	@echo "Data directory cleaned. Run 'make dev' for a fresh start."

# ── Test ────────────────────────────────────────────────────────
test:  ## Run all tests
	$(CARGO) test

# ── Health check ────────────────────────────────────────────────
health:  ## Quick health check via HTTP
	@curl -s http://127.0.0.1:$(PORT)/api/v1/cluster/info | head -c 500
	@echo

# ── Multi-node (local dev cluster) ─────────────────────────────

# Per-node port scheme:  node-N → REST 809N, MC 1121N-1, data /tmp/oxidedb-nodeN

node1: build  ## Start node-1 in background (8091 / 11210)
	@$(MAKE) _start_node N=1 REST_PORT=8091 MC_PORT=11210

node2: build  ## Start node-2 in background (8092 / 11211)
	@$(MAKE) _start_node N=2 REST_PORT=8092 MC_PORT=11211

node3: build  ## Start node-3 in background (8093 / 11212)
	@$(MAKE) _start_node N=3 REST_PORT=8093 MC_PORT=11212

stop1:  ## Stop node-1
	@$(MAKE) _stop_node N=1

stop2:  ## Stop node-2
	@$(MAKE) _stop_node N=2

stop3:  ## Stop node-3
	@$(MAKE) _stop_node N=3

# ── Join nodes to cluster ──────────────────────────────────────
# Usage:
#   make join N=2             → join node-2 to node-1
#   make join N=4 PORT=8094   → join node-4 (custom port) to node-1
#   make join2                → shortcut: join node-2
#   make join3                → shortcut: join node-3
#   make rebalance            → trigger rebalance on node-1

join:  ## Join node-N to cluster (make join N=2)
	@if [ -z "$(N)" ]; then echo "❌ Usage: make join N=<node-number>"; exit 1; fi
	@REST=$${PORT:-$$(( 8090 + $(N) ))}; \
	echo "📡 Joining node-$(N) (REST:$$REST) to node-1 (8091)..."; \
	curl -s -X POST http://127.0.0.1:8091/api/v1/cluster/nodes \
		-H 'Content-Type: application/json' \
		-d "{\"name\":\"node-$(N)\",\"hostname\":\"127.0.0.1\",\"port\":$$REST}" \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print('  ✅', d.get('message', d))" 2>/dev/null \
		|| echo "  ⚠️  join failed"

join1:  ## Join node-1 (no-op, it's the seed node)
	@echo "ℹ️  node-1 is the seed node — already in the cluster."

join2:  ## Join node-2 to cluster
	@$(MAKE) join N=2

join3:  ## Join node-3 to cluster
	@$(MAKE) join N=3

rebalance:  ## Trigger cluster rebalance via node-1
	@echo "⚖️  Triggering rebalance..."
	@curl -s -X POST http://127.0.0.1:8091/api/v1/cluster/rebalance \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print('  ', d.get('message', d))" 2>/dev/null || true

# Start N nodes and auto-join them into a cluster
cluster: build  ## Start a local N-node cluster (default NUM_NODES=3)
	@echo "🚀 Starting $(NUM_NODES)-node local cluster..."
	@for i in $$(seq 1 $(NUM_NODES)); do \
		REST=$$(( 8090 + $$i )); \
		MC=$$(( 11209 + $$i )); \
		$(MAKE) _start_node N=$$i REST_PORT=$$REST MC_PORT=$$MC; \
	done
	@sleep 2
	@echo ""
	@echo "📡 Joining nodes to cluster..."
	@for i in $$(seq 2 $(NUM_NODES)); do \
		REST=$$(( 8090 + $$i )); \
		echo "  → Adding node-$$i (port $$REST) to node-1..."; \
		curl -s -X POST http://127.0.0.1:8091/api/v1/cluster/nodes \
			-H 'Content-Type: application/json' \
			-d "{\"name\":\"node-$$i\",\"hostname\":\"127.0.0.1\",\"port\":$$REST}" \
			| python3 -c "import sys,json; d=json.load(sys.stdin); print('    ✅', d.get('message', d))" 2>/dev/null || echo "    ⚠️  join failed"; \
	done
	@echo ""
	@echo "⚖️  Triggering rebalance..."
	@curl -s -X POST http://127.0.0.1:8091/api/v1/cluster/rebalance \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print('  ', d.get('message', d))" 2>/dev/null || true
	@echo ""
	@$(MAKE) cluster-status

cluster-stop:  ## Stop all cluster nodes
	@echo "🛑 Stopping cluster..."
	@for i in $$(seq 1 $(NUM_NODES)); do \
		$(MAKE) _stop_node N=$$i 2>/dev/null; \
	done
	@echo "All nodes stopped."

cluster-restart: cluster-stop cluster  ## Restart the whole cluster

cluster-status:  ## Show cluster status for all running nodes
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║              Cluster Status                         ║"
	@echo "╠══════════════════════════════════════════════════════╣"
	@for i in $$(seq 1 $(NUM_NODES)); do \
		REST=$$(( 8090 + $$i )); \
		PF=.node-$$i.pid; \
		if [ -f $$PF ] && kill -0 $$(cat $$PF) 2>/dev/null; then \
			STATUS="✅ running (pid=$$(cat $$PF))"; \
		else \
			STATUS="⬚  stopped"; \
		fi; \
		printf "║  node-%-2s  REST:%s  MC:%s  %s\n" "$$i" "$$REST" "$$(( 11209 + $$i ))" "$$STATUS"; \
	done
	@echo "╠══════════════════════════════════════════════════════╣"
	@echo "║  Orchestrator / Chronicle:"
	@curl -s http://127.0.0.1:8091/api/v1/internal/orchestrator 2>/dev/null \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print('║    Leader:', d.get('orchestrator_node','?'), ' Nodes:', d.get('participating_nodes','?'))" 2>/dev/null \
		|| echo "║    (node-1 not reachable)"
	@echo "╚══════════════════════════════════════════════════════╝"

## Graceful cluster shutdown — remove nodes from cluster, then stop processes
cluster-down:  ## Gracefully drain & shut down the entire cluster
	@echo "🔻 Gracefully shutting down cluster..."
	@echo ""
	@echo "📤 Removing nodes from cluster (drain)..."
	@for i in $$(seq $(NUM_NODES) -1 2); do \
		REST=$$(( 8090 + $$i )); \
		PF=.node-$$i.pid; \
		if [ -f $$PF ] && kill -0 $$(cat $$PF) 2>/dev/null; then \
			echo "  → Removing node-$$i from cluster..."; \
			curl -s -X DELETE http://127.0.0.1:8091/api/v1/cluster/nodes/node-$$i 2>/dev/null \
				| python3 -c "import sys,json; d=json.load(sys.stdin); print('    ', d.get('message', d))" 2>/dev/null \
				|| echo "    ⚠️  remove failed (node may already be gone)"; \
		fi; \
	done
	@echo ""
	@echo "⚖️  Rebalancing remaining node..."
	@curl -s -X POST http://127.0.0.1:8091/api/v1/cluster/rebalance 2>/dev/null \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print('  ', d.get('message', d))" 2>/dev/null || true
	@sleep 1
	@echo ""
	@echo "🛑 Stopping all node processes..."
	@for i in $$(seq 1 $(NUM_NODES)); do \
		$(MAKE) _stop_node N=$$i 2>/dev/null; \
	done
	@echo ""
	@echo "✅ Cluster is down."

## Quick alias
down: cluster-down  ## Alias for cluster-down

## Bring cluster back up (build + start + join + rebalance)
cluster-up: cluster  ## Alias for cluster (start fresh cluster)

up: dev  ## Alias — start single node

## Nuclear option — kill all oxidedb processes system-wide
kill-all:  ## Force-kill ALL oxidedb processes
	@echo "💀 Force-killing all oxidedb processes..."
	@pkill -9 -f "target/debug/$(BINARY)" 2>/dev/null || true
	@pkill -9 -f "target/release/$(BINARY)" 2>/dev/null || true
	@rm -f .server.pid .node-*.pid
	@echo "✅ All processes killed, PID files cleaned."

cluster-clean:  ## Stop cluster and remove all node data
	@$(MAKE) cluster-stop
	@for i in $$(seq 1 $(NUM_NODES)); do \
		rm -rf /tmp/oxidedb-node$$i; \
		rm -f .node-$$i.pid .node-$$i.log; \
	done
	@echo "All node data cleaned."

# ── Internal helpers (not meant to be called directly) ─────────
_start_node:
	@PF=.node-$(N).pid; \
	LF=.node-$(N).log; \
	if [ -f $$PF ] && kill -0 $$(cat $$PF) 2>/dev/null; then \
		echo "  node-$(N) already running (pid=$$(cat $$PF))"; \
	else \
		echo "  Starting node-$(N) (REST:$(REST_PORT) MC:$(MC_PORT))..."; \
		RUST_LOG=info ./target/debug/$(BINARY) \
			--port $(REST_PORT) \
			--memcached-port $(MC_PORT) \
			--node-name node-$(N) \
			--data-dir /tmp/oxidedb-node$(N) \
			$(_PERSIST_FLAG) \
			$(EXTRA) \
			> $$LF 2>&1 & echo $$! > $$PF; \
		sleep 1; \
		if kill -0 $$(cat $$PF) 2>/dev/null; then \
			echo "  ✅ node-$(N) running (pid=$$(cat $$PF))"; \
		else \
			echo "  ❌ node-$(N) failed — see $$LF"; \
			rm -f $$PF; \
		fi; \
	fi

_stop_node:
	@PF=.node-$(N).pid; \
	if [ -f $$PF ]; then \
		PID=$$(cat $$PF); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "  Stopping node-$(N) (pid=$$PID)..."; \
			kill $$PID; \
			sleep 1; \
			kill -0 $$PID 2>/dev/null && kill -9 $$PID; \
		fi; \
		rm -f $$PF; \
	fi

# ── Help ────────────────────────────────────────────────────────
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
