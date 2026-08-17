#!/bin/bash
# =============================================================================
# run_daemon_heartbeat_accept.sh — daemon 跨节点判活 + CPU-only check 降级验收
# =============================================================================
# 背景 (2026-08-15 事故): 登录节点 sched daemon status 误判"未运行"——daemon 在
#   计算节点跑, 登录节点 PID namespace 不同, os.kill(pid,0) 看不到 -> 误判 ->
#   submit 误触发 start -> 连锁 nvidia-smi 检查失败.
# 修复: is_running/status_str 心跳为主 (心跳文件在共享 NFS, 跨节点有效);
#       check nvidia-smi 缺失时 config.gpus 空 -> warn (纯 CPU 部署合法).
#
# 覆盖场景:
#   1. fake daemon 启动 -> is_running True (心跳新鲜); stop 后 -> False
#   2. 跨节点模拟: 无 PID 但心跳新鲜 -> is_running True + status_str "运行中"
#   3. 心跳过期 (>60s) -> is_running False
#   4. check 纯 CPU (config.gpus=[]) 无 nvidia-smi -> warn 非 fail
#   5. stop 跨节点提示: 无 PID 但心跳新鲜 -> 提示去计算节点执行
#
# 用法: bash sched/tests/run_daemon_heartbeat_accept.sh
# 退出码: 0 = 全过, 1 = 有失败
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT/sched${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

mk_config() { # $1=state_dir $2=gpus_json
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": $2,
  "projects": {"default": {"root": "$(pwd)", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

is_running() { # $1=state_dir -> 0/1
  local st=$1
  SCHED_STATE=$st SCHED_CONFIG=$st/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import daemon
print(1 if daemon.is_running() else 0)
"
}

status_str() { # $1=state_dir
  local st=$1
  SCHED_STATE=$st SCHED_CONFIG=$st/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import daemon
print(daemon.status_str())
"
}

echo "=== daemon 跨节点判活 + CPU-only check 降级验收 ==="

# ---------- 场景 1: fake daemon 生命周期 ----------
echo "--- 场景 1: fake daemon 启动/停止判活 ---"
S1=/tmp/sched_acc_hb1; rm -rf $S1; mkdir -p $S1
mk_config $S1 "[0]"
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(is_running $S1)" = "1" ] && ok "fake daemon 心跳新鲜 -> 运行中" || bad "fake daemon 未判运行"
$PY -m gsched.cli daemon stop >/dev/null 2>&1
[ "$(is_running $S1)" = "0" ] && ok "stop 后心跳清理 -> 未运行" || bad "stop 后仍判运行"

# ---------- 场景 2+5: 跨节点模拟 (无 PID, 心跳新鲜) ----------
echo "--- 场景 2+5: 无 PID + 心跳新鲜 = 别的主机在跑 ---"
S2=/tmp/sched_acc_hb2; rm -rf $S2; mkdir -p $S2
mk_config $S2 "[0]"
# 模拟: 登录节点视角 —— PID 文件不存在, 但共享 home 里计算节点的心跳新鲜
HB=$S2/testnode/daemon.heartbeat
mkdir -p $(dirname $HB)
touch $HB   # mtime = 现在 (新鲜)
[ "$(is_running $S2)" = "1" ] && ok "跨节点: 心跳新鲜 -> 判运行 (PID 不可见不影响)" \
  || bad "跨节点: 心跳新鲜但未判运行"
status_str $S2 | grep -q "运行中" && ok "status_str 显示运行中 (跨节点)" \
  || bad "status_str 未显示运行中: $(status_str $S2)"
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli daemon stop > $S2/stop_out.txt 2>&1
grep -q "计算节点" $S2/stop_out.txt && ok "stop 跨节点提示去计算节点" \
  || bad "stop 未提示计算节点 (输出: $(cat $S2/stop_out.txt))"

# ---------- 场景 3: 心跳过期 ----------
echo "--- 场景 3: 心跳过期 -> 判死 ---"
S3=/tmp/sched_acc_hb3; rm -rf $S3; mkdir -p $S3
mk_config $S3 "[0]"
HB3=$S3/testnode/daemon.heartbeat
mkdir -p $(dirname $HB3)
touch -d "5 minutes ago" $HB3
[ "$(is_running $S3)" = "0" ] && ok "心跳过期 (>60s) -> 判死" || bad "心跳过期仍判运行"

# ---------- 场景 4: 纯 CPU check 降级 ----------
echo "--- 场景 4: config.gpus=[] 无 nvidia-smi -> warn 非 fail ---"
S4=/tmp/sched_acc_hb4; rm -rf $S4; mkdir -p $S4
mk_config $S4 "[]"
export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json
if command -v nvidia-smi >/dev/null 2>&1; then
  echo "  (本机有 nvidia-smi, 降级分支走 nvidia-smi 可查询路径, 跳过纯 CPU 断言)"
  ok "本机有 nvidia-smi (场景 4 降级分支无法在本机触发, 逻辑见代码)"
else
  OUT=$($PY -m gsched.cli daemon check 2>&1)
  if echo "$OUT" | grep -q "纯 CPU 部署合法"; then
    ok "纯 CPU: nvidia-smi 缺失 -> warn (非 fail)"
  else
    bad "纯 CPU: 未降级为 warn (输出: $OUT)"
  fi
  # fail 数应为 0 (纯 CPU 可通过 check)
  echo "$OUT" | grep -q "FAIL" && bad "纯 CPU check 仍有 FAIL" || ok "纯 CPU check 无 FAIL"
fi
# 对照: config.gpus=[0] 无 nvidia-smi -> fail
if ! command -v nvidia-smi >/dev/null 2>&1; then
  S5=/tmp/sched_acc_hb5; rm -rf $S5; mkdir -p $S5
  mk_config $S5 "[0]"
  export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
  OUT=$($PY -m gsched.cli daemon check 2>&1)
  echo "$OUT" | grep -q "FAIL" && ok "对照: gpus=[0] 无 nvidia-smi -> 仍 fail" \
    || bad "对照: gpus=[0] 应 fail (输出: $OUT)"
fi

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ]
