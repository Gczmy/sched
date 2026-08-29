#!/bin/bash
# B27: 单写者收编验收 —— 非计算节点 submit 走 inbox + daemon 消费入队
set -u
cd "$(dirname "$0")/.."
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

export SCHED_STATE="$(mktemp -d)"
export SCHED_ALLOW_FOREIGN_WRITE=1

NODE="$(uname -n)"
FAKE_NODE="fake-remote-node"
cat > "$SCHED_STATE/config.json" << EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0,1,2,3], "venvs": {"k": "/bin/true"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF

python3 - << 'RPEOF'
import sys, os, json, sqlite3, shutil
sys.path.insert(0, ".")
from gsched import state as st
from gsched.allocator import Allocator
st.init_db()
db = st.db_path()
conn = sqlite3.connect(db)
for i in range(4):
    conn.execute("INSERT INTO gpus (idx, status, quarantined, ignore_until, updated_at, mem_total_gib) VALUES (?, 'free', 0, NULL, datetime('now'), 22.5)", (i,))
conn.commit()

batch_spec = {
    "schema_version": 1,
    "name": "inbox_smoke",
    "project": "p",
    "cwd": "/tmp",
    "tasks": [{"id": "t1", "cmd": ["echo", "hi"], "gpus": 1}],
}

# 模拟: 外部节点投递 (写 inbox 文件 + 插 control_requests)
inbox = os.path.join(os.environ["SCHED_STATE"], st.hostname(), "submit_inbox")
os.makedirs(inbox, exist_ok=True)
payload = os.path.join(inbox, "submit-inbox_smoke-TEST.json")
json.dump({"spec": batch_spec, "bid": "inbox_smoke-TEST0001"}, open(payload, "w"))
with st.connect() as c:
    c.execute("INSERT INTO control_requests (job_id, op, status, created_at) VALUES (?, 'batch_submit', 'pending', datetime('now'))", (payload,))

# daemon 端消费: Dispatcher.__new__ 绕过 __init__, 只挂消费需要的状态
import tempfile
os.makedirs(os.path.join(os.environ["SCHED_STATE"], "localhost"), exist_ok=True)
from gsched.dispatcher import Dispatcher
d = Dispatcher.__new__(Dispatcher)
d.cfg = json.load(open(os.path.join(os.environ["SCHED_STATE"], "config.json")))
d.log_lines = []
def _log(msg): d.log_lines.append(str(msg))
d.log_line = _log
class _FakeExec:
    pass
d.executor = None  # 消费路径不 touch executor

# 构造最少依赖后调用 _process_control_requests
import gsched.state as st2
# allocator 属性仅在 cancel 分支用——batch_submit 分支不触及
try:
    # 需要 self._notify_threads? 不, _process_control_requests 不用
    with open("/tmp/ns_cfg.json","w") as f:
        f.write(json.dumps(d.cfg))
    # 手动调用目标方法所在类的未绑定方法
    Dispatcher._process_control_requests(d)
except Exception as e:
    import traceback; traceback.print_exc(); sys.exit(1)

# 验证: 批次已入队, control_request 完成, inbox 文件已删除
rows = conn.execute("SELECT id,status FROM batches WHERE name='inbox_smoke'").fetchall()
if len(rows) != 1 or rows[0][1] != "queued":
    print(f"❌ 批次未正确入队: {rows}"); sys.exit(1)
print("✅ S1 daemon 消费 inbox -> 批次 queued 入库")
n_tasks = conn.execute("SELECT COUNT(*) FROM tasks WHERE batch_id=?", (rows[0][0],)).fetchone()[0]
print("✅ S2 任务行写入" if n_tasks == 1 else f"❌ 任务数={n_tasks}")
reqs = conn.execute("SELECT status FROM control_requests WHERE op='batch_submit'").fetchall()
print("✅ S3 控制请求已完成" if reqs and reqs[0][0] == "done" else "❌ 控制请求状态异常")
if os.path.exists(payload):
    print("⚠️ inbox payload 未清理 (非致命)")
else:
    print("✅ S4 inbox payload 已清理")

def queue_payload(name, depends_on, bid):
    path = os.path.join(inbox, f"submit-{name}.json")
    spec = {**batch_spec, "name": name, "depends_on": depends_on}
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"spec": spec, "bid": bid}, f)
    with st.connect() as c:
        c.execute(
            "INSERT INTO control_requests (job_id, op, status, created_at) "
            "VALUES (?, 'batch_submit', 'pending', datetime('now'))",
            (path,),
        )
    return path

bad_dep = queue_payload("missing_dep", ["not_created"], "missing-dep-1")
cycle = queue_payload("cycle_dep", ["cycle_dep"], "cycle-dep-1")
Dispatcher._process_control_requests(d)
check = sqlite3.connect(db)
assert check.execute("SELECT 1 FROM batches WHERE name='missing_dep'").fetchone() is None
assert check.execute("SELECT 1 FROM batches WHERE name='cycle_dep'").fetchone() is None
for bad_path in (bad_dep, cycle):
    assert not os.path.exists(bad_path), bad_path
    assert check.execute(
        "SELECT status FROM control_requests WHERE job_id=?", (bad_path,)
    ).fetchone()[0] == "done"
print("✅ S5 daemon rejects missing/cyclic inbox dependencies")
malformed = os.path.join(inbox, "submit-malformed.json")
with open(malformed, "w", encoding="utf-8") as f:
    json.dump([], f)
with st.connect() as c:
    c.execute(
        "INSERT INTO control_requests (job_id, op, status, created_at) "
        "VALUES (?, 'batch_submit', 'pending', datetime('now'))",
        (malformed,),
    )
Dispatcher._process_control_requests(d)
assert check.execute(
    "SELECT status FROM control_requests WHERE job_id=?", (malformed,)
).fetchone()[0] == "done"
assert not os.path.exists(malformed)
print("✅ S5b daemon rejects malformed inbox envelope")

transient = queue_payload("transient_submit", [], "transient-submit-1")
original_insert_batch = st2.insert_batch
failed_once = [False]
def fail_once(*args, **kwargs):
    if not failed_once[0]:
        failed_once[0] = True
        raise sqlite3.OperationalError("simulated busy")
    return original_insert_batch(*args, **kwargs)
st2.insert_batch = fail_once
Dispatcher._process_control_requests(d)
st2.insert_batch = original_insert_batch
assert check.execute(
    "SELECT status FROM control_requests WHERE job_id=?", (transient,)
).fetchone()[0] == "pending"
assert os.path.exists(transient)
Dispatcher._process_control_requests(d)
assert check.execute("SELECT status FROM batches WHERE name='transient_submit'").fetchone()
assert not os.path.exists(transient)
print("✅ S6 transient inbox failure remains pending and retries")
sys.exit(0)
RPEOF

if [ $? -eq 0 ]; then ok "B27 单写者收编"; else bad "B27 单写者收编"; fi

echo ""
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ]
