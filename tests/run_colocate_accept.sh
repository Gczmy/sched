#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_colocate_accept.sh — co-location 共享装箱验收 (定案 39, fake 显存模拟)
# =============================================================================
# 覆盖场景 (文档 §3.2e F + 定案 40 Least-Loaded):
#   S1 同卡 2 任务共存 + 1 结束卡保持 assigned (计数释放, co-tenant 不误杀)
#   S2 最后任务结束 -> releasing -> free (最后任务结束才释放)
#   S3 Least-Loaded 装箱: 同大小任务均匀分散 (平局取最小 idx) / 显存超限换卡 / 全超限等待
#   S4 动态加入: 新任务 pack 到有余量卡 (无需等批次结束)
#   S5 迁移后独占不变 + 组合缺格 (gpu_share × co_locate=false -> 独占+告警)
#   S6 L3 冻结: 装箱显存 > freeze_pct -> 该卡不再 pack (独占任务不受影响)
#   S7 均衡 (5×0.6GiB -> 0/1/2/3/0 不堆首卡) + 独占卡 (vram_gib NULL) 不 pack
#
# 用法: bash sched/tests/run_colocate_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

echo "=== co-location 共享装箱验收 (fake-gpu 显存模拟) ==="

# ---------- S1/S2/S3: 装箱核心 (直接调 _assign_in_tx 单元验证) ----------
echo "--- S1/S2/S3: Least-Loaded 装箱 + 计数释放 ---"
S1=/tmp/sched_coloc_s1; rm -rf $S1; mkdir -p $S1
SCHED_STATE=$S1 SCHED_FAKE_GPUS="0:24,1:24" $PY - <<'EOF'
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

# S1: 手动构造同卡 2 co-tenant (计数释放语义: release 一个不释放卡)
with st.connect() as conn:
    conn.execute("UPDATE gpus SET status='assigned', job_id='job0' WHERE idx=0")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0,'job0',2,datetime('now'))")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0,'job1',2,datetime('now'))")
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
assert al.settle_releasing() == ([0], [])  # fake settle -> free
print('S1/S2 OK')
# S3: Least-Loaded 装箱 (定案 40): 同大小任务均匀分散, 平局取最小 idx
#   a0->0 (全 free 平局取小), a1->1 (GPU1 更空), a2->0 (平局取小), a3->1,
#   a4(12GiB)->0 (平局取小, 4+12=16<=16.8), a5(20GiB)->None (全卡超限)
with st.connect() as conn:
    assert d._assign_in_tx(conn, 'a0', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
    assert d._assign_in_tx(conn, 'a1', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 1
    assert d._assign_in_tx(conn, 'a2', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 0
    assert d._assign_in_tx(conn, 'a3', {'resources': {'gpu_share': True, 'vram_gib': 2}}) == 1
    assert d._assign_in_tx(conn, 'a4', {'resources': {'gpu_share': True, 'vram_gib': 12}}) == 0
    assert d._assign_in_tx(conn, 'a5', {'resources': {'gpu_share': True, 'vram_gib': 20}}) is None
    print('S3 OK')
print('S1/S2/S3 OK')
EOF
if [ $? -eq 0 ]; then ok "S1/S2/S3: 计数释放 + Least-Loaded 装箱 (均衡分散, 平局取小 idx)"; else bad "S1/S2/S3 失败"; fi

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
  "name": "coloc_a",
  "project": "default", "mode": "mix",
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
  "name": "coloc_b",
  "project": "default", "mode": "mix",
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
LA=$(grep -c 'LAUNCH job coloc_a-.*gpu=0' $S4/testnode/scheduler.log 2>/dev/null || echo 0)
LB=$(grep -c 'LAUNCH job coloc_b-.*gpu=0' $S4/testnode/scheduler.log 2>/dev/null || echo 0)
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

# ---------- S7: Least-Loaded 均衡 + 独占卡保护 (定案 40) ----------
echo "--- S7: Least-Loaded 均衡 + 独占卡保护 ---"
S7=/tmp/sched_coloc_s7; rm -rf $S7; mkdir -p $S7
SCHED_STATE=$S7 SCHED_FAKE_GPUS="0:24,1:24,2:24,3:24" $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0, 1, 2, 3])
from gsched.allocator import Allocator
al = Allocator([0, 1, 2, 3], fake=True)
al.probe_capacity()
from gsched.dispatcher import Dispatcher
d = Dispatcher({'co_locate': True, 'co_locate_safety': 0.7, 'co_locate_max_jobs': 3,
                'gpus': [0, 1, 2, 3], 'venvs': {}, 'default_project': '{ROOT}'}, fake=True)
# 5 个 0.6GiB raft 轻任务 -> 均匀分散 0/1/2/3/0, 不堆首卡
with st.connect() as conn:
    got = [d._assign_in_tx(conn, f'j{i}', {'resources': {'gpu_share': True, 'vram_gib': 0.6}}) for i in range(5)]
assert got == [0, 1, 2, 3, 0], f'均衡失败: {got}'
# 独占卡保护: GPU3 已有独占任务 (vram_gib NULL) -> 新共享任务不 pack, 去最空卡
with st.connect() as conn:
    conn.execute("UPDATE gpus SET status='assigned', job_id='excl' WHERE idx=3")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (3,'excl',NULL,datetime('now'))")
    g = d._assign_in_tx(conn, 'j5', {'resources': {'gpu_share': True, 'vram_gib': 0.6}})
# 分布 [0,1,2,3,0] -> GPU0=1.2, GPU1/2/3=0.6; GPU3 独占(视为满) -> 最空 GPU1/2 (0.6)
# 平局取最小 idx -> GPU1. 独占卡 GPU3 不被 pack (保护生效)
assert g == 1, f'独占卡被 pack 或选卡错: {g} (应为 1)'
print('S7 OK')
EOF
if [ $? -eq 0 ]; then ok "S7: Least-Loaded 均衡 (5×0.6 -> 0/1/2/3/0) + 独占卡不 pack"; else bad "S7 失败"; fi

# ---------- S8: 异构容量归一化负载 (16GB + 24GB 混用, 方案 A) ----------
# 16GB@4GiB(load 0.25) vs 24GB@8GiB(load 0.33): 放 8GiB 任务后 16GB->0.75 / 24GB->0.67
# 归一化负载应选 24GB 卡 (绝对 used 8>4 会误选 16GB 卡 -> 大卡空转)
echo "--- S8: 异构容量归一化负载 (16+24 混用) ---"
S8=/tmp/sched_coloc_s8; rm -rf $S8; mkdir -p $S8
SCHED_STATE=$S8 SCHED_FAKE_GPUS="0:16,1:24" $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0, 1])
from gsched.allocator import Allocator
al = Allocator([0, 1], fake=True)
al.probe_capacity()
assert al.mem_total(0) == 16 and al.mem_total(1) == 24, (al.mem_total(0), al.mem_total(1))
from gsched.dispatcher import Dispatcher
d = Dispatcher({'co_locate': True, 'co_locate_safety': 0.7, 'co_locate_max_jobs': 3,
                'gpus': [0, 1], 'venvs': {}, 'default_project': '{ROOT}'}, fake=True)
with st.connect() as conn:
    # 预置: 16GB 卡已用 4GiB (load 0.25) / 24GB 卡已用 8GiB (load 0.33)
    conn.execute("UPDATE gpus SET status='assigned' WHERE idx=0")
    conn.execute("UPDATE gpus SET status='assigned' WHERE idx=1")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0,'a',4,datetime('now'))")
    conn.execute("INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (1,'b',8,datetime('now'))")
    # 8GiB 任务: 放 16GB 卡 load=(4+8)/16=0.75; 放 24GB 卡 load=(8+8)/24=0.67 -> 选 24GB (卡 1)
    g = d._assign_in_tx(conn, 'big8', {'resources': {'gpu_share': True, 'vram_gib': 8}})
    assert g == 1, f'归一化负载应选 24GB 卡, got {g}'
    # 容量硬约束仍逐卡正确: 16GB 卡 4+14=18 > 16*0.7=11.2 -> 拒绝; 24GB 卡 8+14=22 > 16.8 -> 也拒绝
    assert d._assign_in_tx(conn, 'too14', {'resources': {'gpu_share': True, 'vram_gib': 14}}) is None
print('S8 OK')
EOF
if [ $? -eq 0 ]; then ok "S8: 异构归一化负载 - 24GB 卡 load 更低被选中 (不堆小卡) + 容量硬约束逐卡正确"; else bad "S8 失败"; fi

# ---------- S9: 独占容量适配 (方案 B: 声明 vram 跳过容量不足卡) ----------
echo "--- S9: 独占容量适配 ---"
S9=/tmp/sched_coloc_s9; rm -rf $S9; mkdir -p $S9
SCHED_STATE=$S9 SCHED_FAKE_GPUS="0:16,1:24" $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0, 1])
from gsched.allocator import Allocator
al = Allocator([0, 1], fake=True)
al.probe_capacity()
from gsched.dispatcher import Dispatcher
d = Dispatcher({'gpus': [0, 1], 'venvs': {}, 'default_project': '{ROOT}'}, fake=True)
with st.connect() as conn:
    # 独占任务声明 vram 20GiB: 16GB 卡装不下 -> 跳过 -> 派到 24GB 卡
    g = d._assign_in_tx(conn, 'big', {'resources': {'vram_gib': 20}})
    assert g == 1, f'20GiB 任务应跳过 16GB 卡派到 24GB 卡, got {g}'
    # 未声明 vram: 维持现状 (不声明不校验) -> 第一张 free 卡 (卡 0)
    g2 = d._assign_in_tx(conn, 'no_decl', {})
    assert g2 == 0, f'未声明维持现状应取第一张 free 卡, got {g2}'
print('S9 OK')
EOF
if [ $? -eq 0 ]; then ok "S9: 独占容量适配 - 声明 vram 20GiB 跳过 16GB 卡选 24GB 卡 + 未声明维持现状"; else bad "S9 失败"; fi

# ---------- S6: L3 冻结 ----------
echo "--- S6: L3 冻结 ---"
S6=/tmp/sched_coloc_s6; rm -rf $S6; mkdir -p $S6
SCHED_STATE=$S6 SCHED_FAKE_GPUS="0:24" $PY - <<'EOF'
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
