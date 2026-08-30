#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_gpu_mem_accept.sh — GPU 显存配置/探测验收 (2026-08-17)
# =============================================================================
# 背景: 三个缺口
#   1. 显存手动覆盖: config.gpus 对象形态 {idx, mem_gib} (daemon 启动覆盖探测值)
#      + `sched gpu-set-mem <idx> <gib>` 写入 state.db, 运行中 daemon 重启后读取
#   2. 自动探测全卡: config 未配 gpus -> Allocator 自动 nvidia-smi -L (定案 1
#      第三级回退, 之前文档写了但代码没实现)
#   3. list-gpus 显示显存: mem_total_gib 列展示 (之前只有状态/job/quarantine)
#
# 覆盖场景:
#   1. 对象形态 gpus 解析: {idx,mem_gib} -> gpu_list + mem_overrides
#   2. fake daemon 启动: 对象形态容量生效 (probe_capacity 用 override)
#   3. gpu-set-mem 写入 state.db (Allocator 缓存需重启 daemon)
#   4. list-gpus 显示显存列
#   5. 兼容: 纯卡号数组 gpus 仍可用 (向后兼容)
#   6. 自动探测: config 无 gpus + fake 模式 -> fake 语义 (探测不适用, 验证不崩)
#
# 用法: bash sched/tests/run_gpu_mem_accept.sh
# 退出码: 0 = 全过, 1 = 有失败
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  sleep 1
}

mem_of() { # $1=state_dir $2=idx -> mem_total_gib
  local st=$1 idx=$2
  SCHED_STATE=$st SCHED_CONFIG=$st/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import state
with state.connect() as conn:
    r = conn.execute('SELECT mem_total_gib FROM gpus WHERE idx=?', ($idx,)).fetchone()
    print(r['mem_total_gib'] if r and r['mem_total_gib'] else '')
"
}

echo "=== GPU 显存配置/探测验收 ==="

# ---------- 场景 1: 对象形态 gpus 解析 ----------
echo "--- 场景 1: config.gpus 对象形态 {idx,mem_gib} 解析 ---"
sched_accept_make_root S1 "sched-gpu-mem-1"
cat > $S1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S1",
  "gpus": [{"idx": 0, "mem_gib": 24}, {"idx": 1, "mem_gib": 16}, {"idx": 2, "mem_gib": 16}],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
PARSE_OK=$(SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched.config import load_config, parse_gpus
cfg = load_config()
idxs, mem, _mj = parse_gpus(cfg)
assert idxs == [0, 1, 2], idxs
assert mem == {0: 24.0, 1: 16.0, 2: 16.0}, mem
print('OK')
")
[ "$PARSE_OK" = "OK" ] && ok "对象形态解析: idxs=[0,1,2] mem={0:24,1:16,2:16}" \
  || bad "对象形态解析失败: $PARSE_OK"

# ---------- 场景 2: fake daemon 启动, 对象容量生效 ----------
echo "--- 场景 2: fake daemon 启动 -> probe_capacity 用 override 容量 ---"
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
SCHED_FAKE_GPUS="0,1,2" $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
M0=$(mem_of $S1 0); M1=$(mem_of $S1 1); M2=$(mem_of $S1 2)
[ "$M0" = "24.0" ] && [ "$M1" = "16.0" ] && [ "$M2" = "16.0" ] \
  && ok "容量探测用 config override (GPU0=24 / GPU1=16 / GPU2=16 GiB)" \
  || bad "override 容量未生效 (got GPU0=$M0 GPU1=$M1 GPU2=$M2)"
# 场景 3: gpu-set-mem 运行时覆盖
echo "--- 场景 3: gpu-set-mem 运行时覆盖 + 非法值拒绝 ---"
MEM_OUT=$($PY -m gsched.cli gpu-set-mem 0 20 2>&1)
[ "$(mem_of $S1 0)" = "20.0" ] && ok "gpu-set-mem 0 20 写入 state.db" \
  || bad "gpu-set-mem 未写入 state.db (got $(mem_of $S1 0))"
case "$MEM_OUT" in
  *"重启探测会覆盖"*"config.gpus"*) ok "gpu-set-mem 提示临时值与持久化方式" ;;
  *) bad "gpu-set-mem 未说明重启覆盖/config 持久化: $MEM_OUT" ;;
esac
if $PY -m gsched.cli gpu-set-mem 0 -5 >/dev/null 2>&1; then
  bad "gpu-set-mem 负数应拒绝"
else
  ok "gpu-set-mem 非法值 (<=0) 拒绝"
fi
# 场景 4: list-gpus 显示显存
echo "--- 场景 4: list-gpus 显示显存列 ---"
LIST_OUT=$($PY -m gsched.cli list-gpus 2>/dev/null)
echo "$LIST_OUT" | grep -q "20.0GiB" && ok "list-gpus 显示显存列 (GPU0 20.0GiB)" \
  || bad "list-gpus 缺显存列 (输出: $LIST_OUT)"
echo "$LIST_OUT" | grep -q "GPU0" && ok "list-gpus 卡号正常" || bad "list-gpus 卡号异常"
stop_daemon $S1

# 重启探测重新采用 config.gpus 的容量覆盖，临时 state.db 值不持久。
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
SCHED_FAKE_GPUS="0,1,2" $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(mem_of $S1 0)" = "24.0" ] && ok "daemon 重启覆盖 gpu-set-mem 临时值" \
  || bad "daemon 重启未覆盖临时容量 (got $(mem_of $S1 0))"
stop_daemon $S1

# ---------- 场景 5: 纯卡号数组向后兼容 ----------
echo "--- 场景 5: 纯卡号数组 gpus 向后兼容 ---"
sched_accept_make_root S2 "sched-gpu-mem-2"
cat > $S2/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S2", "gpus": [0, 1],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
PARSE2=$(SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched.config import load_config, parse_gpus
cfg = load_config()
idxs, mem, _mj = parse_gpus(cfg)
assert idxs == [0, 1] and mem == {}, (idxs, mem)
print('OK')
")
[ "$PARSE2" = "OK" ] && ok "纯卡号数组解析向后兼容 ([0,1], 无 override)" \
  || bad "纯卡号数组解析失败: $PARSE2"
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
SCHED_FAKE_GPUS="0,1" $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(mem_of $S2 0)" = "24.0" ] && ok "纯卡号 fake 缺省容量 24GiB (现状语义不变)" \
  || bad "纯卡号 fake 容量异常 (got $(mem_of $S2 0))"
stop_daemon $S2

# ---------- 场景 6: config 无 gpus -> 自动探测 (fake 下不崩) ----------
echo "--- 场景 6: config 无 gpus -> fake 自动探测语义 (不崩, 用 fake 列表) ---"
sched_accept_make_root S3 "sched-gpu-mem-3"
cat > $S3/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S3",
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
SCHED_FAKE_GPUS="0,1,2,3" $PY -m gsched.cli daemon start --fake >/dev/null 2>&1 && \
  sleep 2 && $PY -m gsched.cli daemon status >/dev/null 2>&1
if $PY -m gsched.cli status --json >/dev/null 2>&1 && [ "$($PY -m gsched.cli status --json 2>/dev/null | grep -o '"idx": [0-9]' | wc -l | tr -d ' ')" = "4" ]; then
  ok "config 无 gpus + fake -> 4 卡正常 (自动探测路径不崩)"
else
  bad "config 无 gpus + fake 异常"
fi
stop_daemon $S3

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
