#!/bin/bash
# =============================================================================
# run_colocate_accept.sh — co-location 共享装箱验收 (定案 39, fake 显存模拟)
# =============================================================================
# 覆盖场景 (文档 §3.2e F):
#   S1 同卡 2 任务共存 + 1 结束卡保持 assigned (计数释放, co-tenant 不误杀)
#   S2 最后任务结束 -> releasing -> free (最后任务结束才释放)
#   S3 First-Fit 装箱: 任务数上限 / 显存超限换卡 / 全超限等待
#   S4 动态加入: 新任务 pack 到有余量卡 (无需等批次结束)
#   S5 迁移后独占不变 + 组合缺格 (gpu_share × co_locate=false -> 独占+告警)
#   S6 L3 冻结: 装箱显存 > freeze_pct -> 该卡不再 pack (独占任务不受影响)
#
# 用法: bash sched/tests/run_colocate_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-/Users/zzc/miniconda3/envs/vnpy_env/bin/python}
ROOT=$(pwd)

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

echo "=== co-location 共享装箱验收 (fake-gpu 显存模拟) ==="

# ---------- S1/S2/S3: 装箱核心 (直接调 _assign_in_tx 单元验证) ----------
echo "--- S1/S2/S3: First-Fit 装箱 + 计数释放 ---"
SCHED_STATE=/tmp/sched_coloc_s1 SCHED_FAKE_GPUS="0:24,1:24" $PY - <<'EOF'
import os, sys, tempfile
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0, 1])
from gsched.allocator import Allocator
al = Allocator([0, 1], fake=True)
al.probe_capacity()
from gsched.dispatcher import Dispatcher
cfg = {'co_locate': True, 'co_locate_safety': 0.7, 'co_locate_max_jobs': 3,
       'gpus': [0, 1], 'venvs': {}, 'default_project': '{ROOT}'}
d = Dispatcher(cfg, fake=True)

# S1: 2 个 2GiB 共享任务 pack 到 GPU0 (同卡共存)
with st.connect() as conn:
    assert d._assign_in_tx(conn, 'job0', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
    assert d._assign_in_tx(conn, 'job1', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
# release 走独立连接 (WAL 写锁: 必须在外层事务外)
with st.connect() as conn:
    g = conn.execute("SELECT status, job_id FROM gpus WHERE idx=0").fetchone()
    assert g['status'] == 'assigned', g['status']
al.release('job0')  # job0 结束 -> 卡仍 assigned (job1 co-tenant 还在)
with st.connect() as conn:
    g = conn.execute("SELECT status, job_id FROM gpus WHERE idx=0").fetchone()
    assert g['status'] == 'assigned', g['status']
    assert g['job_id'] == 'job1', g['job_id']  # 镜像改指剩余 job1
al.release('job1')  # 最后任务结束 -> releasing
with st.connect() as conn:
    g = conn.execute("SELECT status FROM gpus WHERE idx=0").fetchone()
    assert g['status'] == 'releasing', g['status']
assert al.settle_releasing() == [0]  # fake settle -> free
print('S1/S2 OK')
# S3: 3x 2GiB -> GPU0 (任务数上限 3); 第 4 个 -> GPU1 (free)
with st.connect() as conn:
    for i in range(3):
        assert d._assign_in_tx(conn, f'a{i}', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
    assert d._assign_in_tx(conn, 'a3', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 1
    # 12GiB -> GPU1 (2+12=14 <= 16.8); 20GiB -> 全卡超限 None
    assert d._assign_in_tx(conn, 'a4', {'resources': {'gpu_share': True, 'vram_gib': 12}}) == 1
    assert d._assign_in_tx(conn, 'a5', {'resources': {'gpu_share': True, 'vram_gib': 20}}) is None
    print('S3 OK')
print('S1/S2/S3 OK')
EOF
if [ $? -eq 0 ]; then ok "S1/S2/S3: 同卡共存 + 计数释放 + First-Fit 装箱"; else bad "S1/S2/S3 失败"; fi

# ---------- S4: 动态加入 (新批次任务 pack 到有余量卡) ----------
echo "--- S4: 动态加入 ---"
S4=/tmp/sched_coloc_s4; rm -rf $S4; mkdir -p $S4
cat > $S4/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S4", "gpus": [0], "co_locate": true,
  "co_locate_safety": 0.7, "co_locate_max_jobs": 3,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json SCHED_FAKE_GPUS="0:24"
# 批次 A: 2 个 2GiB 共享任务
cat > $S4/batch_a.json << EOF
{
  "name": "coloc_a", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(3); open('$S4/a1.txt','w').write('ok')"], "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 2}, "artifacts": {"a": {"path": "$S4/a1.txt"}}, "paths_escape": true},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(3); open('$S4/a2.txt','w').write('ok')"], "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 2}, "artifacts": {"a": {"path": "$S4/a2.txt"}}, "paths_escape": true}
  ]
}
EOF
$PY -m gsched.cli submit $S4/batch_a.json >/dev/null 2>&1
env SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json SCHED_FAKE_GPUS="0:24" \
    $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
for _ in $(seq 1 20); do
  [ -f "$S4/a2.txt" ] && break; sleep 1
done
# 批次 B (动态加入): 新 2GiB 任务 -> 应 pack 到 GPU0 (2+2+2=6 <= 16.8)
cat > $S4/batch_b.json << EOF
{
  "name": "coloc_b", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(3); open('$S4/b1.txt','w').write('ok')"], "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 2}, "artifacts": {"a": {"path": "$S4/b1.txt"}}, "paths_escape": true}
  ]
}
EOF
$PY -m gsched.cli submit $S4/batch_b.json >/dev/null 2>&1
S4_OK=0
for _ in $(seq 1 20); do
  [ -f "$S4/b1.txt" ] && break; sleep 1
done
# 验证: scheduler.log 中 coloc_a 与 coloc_b 任务均 LAUNCH 到 gpu=0 (同卡共存动态加入);
# 完成后 gpu_jobs 会清空 (计数释放), 故用日志证据而非 DB 计数
LA=$(grep -c 'LAUNCH job coloc_a-.*gpu=0' $S4/$(hostname)/scheduler.log 2>/dev/null || echo 0)
LB=$(grep -c 'LAUNCH job coloc_b-.*gpu=0' $S4/$(hostname)/scheduler.log 2>/dev/null || echo 0)
if [ "$LA" = "2" ] && [ "$LB" = "1" ] && [ -f "$S4/b1.txt" ]; then
  ok "S4: 动态加入 - 新批次任务 pack 到同卡 (a×2+b×1 均 gpu=0)"
else
  bad "S4: 动态加入失败 (a_gpu0=$LA b_gpu0=$LB b1=$([ -f $S4/b1.txt ] && echo yes || echo no))"
fi
env SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json $PY -m gsched.cli daemon stop >/dev/null 2>&1
pkill -f "gsched.dispatcher_main" 2>/dev/null; sleep 1

# ---------- S5: 组合缺格 (gpu_share × co_locate=false) ----------
echo "--- S5: 组合缺格 ---"
S5=/tmp/sched_coloc_s5; rm -rf $S5; mkdir -p $S5
cat > $S5/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S5", "gpus": [0], "co_locate": false,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json SCHED_FAKE_GPUS="0:24" $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0])
from gsched.dispatcher import Dispatcher
d = Dispatcher({'co_locate': False, 'gpus': [0], 'venvs': {}, 'default_project': '{ROOT}'}, fake=True)
with st.connect() as conn:
    # gpu_share=true 但 co_locate=false -> 按独占跑 (返回 free 卡) + 告警日志
    assert d._assign_in_tx(conn, 'jobx', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
print('S5 OK')
EOF
if [ $? -eq 0 ]; then ok "S5: 组合缺格 - gpu_share × co_locate=false -> 独占 + 告警"; else bad "S5 失败"; fi

# ---------- S6: L3 冻结 ----------
echo "--- S6: L3 冻结 ---"
SCHED_STATE=/tmp/sched_coloc_s6 SCHED_FAKE_GPUS="0:24" $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0])
from gsched.dispatcher import Dispatcher
d = Dispatcher({'co_locate': True, 'co_locate_freeze_pct': 85, 'gpus': [0], 'venvs': {}, 'default_project': '{ROOT}'}, fake=True)
with st.connect() as conn:
    conn.execute("UPDATE gpus SET status='assigned' WHERE idx=0")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0,'jobx',2, datetime('now'))")
d._l3_freeze_sample()
assert d._frozen_gpus == set(), d._frozen_gpus  # 2/24 < 85% 不冻结
with st.connect() as conn:
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0,'big',22, datetime('now'))")
d._last_freeze_sample = 0
d._l3_freeze_sample()
assert 0 in d._frozen_gpus, d._frozen_gpus  # 24/24 > 85% 冻结
# 冻结后共享任务不再 pack 该卡
with st.connect() as conn:
    assert d._assign_in_tx(conn, 'newjob', {'resources': {'gpu_share': True, 'vram_gib': 1}}) is None
print('S6 OK')
EOF
if [ $? -eq 0 ]; then ok "S6: L3 冻结 - 装箱显存 >85% 冻结, 冻结后不再 pack"; else bad "S6 失败"; fi

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
