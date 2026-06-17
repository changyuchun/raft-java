# Raft 工作流程完整分析

> 基于 `raft-java` 源码精确还原，覆盖正常场景与异常场景，每步骤标注入参/出参。

---

## 目录
1. [核心状态机与关键变量](#1-核心状态机与关键变量)
2. [场景一：Leader 选举（正常）](#2-场景一leader-选举正常)
3. [场景二：Leader 选举（异常）](#3-场景二leader-选举异常)
4. [场景三：日志复制（正常）](#4-场景三日志复制正常)
5. [场景四：日志复制（异常——日志冲突）](#5-场景四日志复制异常日志冲突)
6. [场景五：心跳保活（正常）](#6-场景五心跳保活正常)
7. [场景六：快照生成（正常）](#7-场景六快照生成正常)
8. [场景七：快照安装（Follower 日志落后太多）](#8-场景七快照安装follower-日志落后太多)
9. [场景八：客户端写请求完整链路](#9-场景八客户端写请求完整链路)
10. [场景九：节点崩溃恢复](#10-场景九节点崩溃恢复)
11. [消息格式速查](#11-消息格式速查)

---

## 1. 核心状态机与关键变量

```
┌─────────────────────────────────────────────────────────────┐
│                        RaftNode 核心状态                      │
├──────────────────────┬──────────────────────────────────────┤
│ 变量                  │ 含义                                  │
├──────────────────────┼──────────────────────────────────────┤
│ state                │ FOLLOWER / PRE_CANDIDATE /            │
│                      │ CANDIDATE / LEADER                    │
│ currentTerm          │ 当前任期号（持久化）                    │
│ votedFor             │ 本任期投票给谁（持久化，0=未投）         │
│ leaderId             │ 当前 leader 的 id                      │
│ commitIndex          │ 已提交日志最大 index                    │
│ lastAppliedIndex     │ 已应用到状态机的最大 index（volatile）   │
├──────────────────────┼──────────────────────────────────────┤
│ Peer.nextIndex       │ Leader 认为该 peer 下一条需要发的 index  │
│ Peer.matchIndex      │ Leader 确认该 peer 已复制的最大 index   │
└──────────────────────┴──────────────────────────────────────┘

节点状态转换：
  FOLLOWER ──选举超时──→ PRE_CANDIDATE
  PRE_CANDIDATE ──获得多数预投票──→ CANDIDATE
  CANDIDATE ──获得多数投票──→ LEADER
  LEADER/CANDIDATE ──收到更高 term──→ FOLLOWER
```

---

## 2. 场景一：Leader 选举（正常）

**触发条件**：Follower 的选举计时器超时（默认 5000~10000ms 随机）

### 2.1 Pre-Vote 阶段

```mermaid
sequenceDiagram
    participant F as Follower (发起方)
    participant A as Peer A
    participant B as Peer B

    Note over F: 选举超时触发 startPreVote()
    Note over F: state → PRE_CANDIDATE

    Note right of F: 构造 VoteRequest
    Note right of F: 入参：serverId, term=currentTerm,<br/>lastLogIndex, lastLogTerm

    F->>A: preVote(VoteRequest)
    F->>B: preVote(VoteRequest) [并发发送]

    Note over A: 检查：<br/>1. serverId 在配置中？<br/>2. request.term >= currentTerm?<br/>3. request.lastLogTerm/Index 够新？
    A-->>F: VoteResponse{granted=true, term=T}

    Note over B: 同样检查
    B-->>F: VoteResponse{granted=true, term=T}

    Note over F: PreVoteResponseCallback.success()<br/>计票: voteGrantedNum=3 > 3/2=1<br/>→ 调用 startVote()
```

**Pre-Vote 入参（VoteRequest）**

| 字段 | 来源 | 示例值 |
|------|------|--------|
| `server_id` | localServer.getServerId() | `1` |
| `term` | currentTerm（**不加1**） | `5` |
| `last_log_index` | raftLog.getLastLogIndex() | `42` |
| `last_log_term` | getLastLogTerm() | `5` |

**Pre-Vote 出参（VoteResponse）**

| 字段 | 含义 | 示例值 |
|------|------|--------|
| `granted` | 是否同意 | `true` |
| `term` | 响应方当前 term | `5` |

**Peer 侧授权逻辑（RaftConsensusServiceImpl.preVote）**

```
拒绝条件（返回 granted=false）：
  1. serverId 不在 configuration 里
  2. request.term < currentTerm
  3. request.lastLogTerm < myLastLogTerm
     OR (lastLogTerm 相同 && request.lastLogIndex < myLastLogIndex)

授权条件：以上都不满足 → granted=true
```

---

### 2.2 正式 Vote 阶段

```mermaid
sequenceDiagram
    participant C as Candidate (发起方)
    participant A as Peer A
    participant B as Peer B

    Note over C: startVote() 被调用
    Note over C: currentTerm++ (term=6)<br/>state → CANDIDATE<br/>votedFor = selfId<br/>leaderId = 0

    Note right of C: 构造 VoteRequest
    Note right of C: 入参：serverId=1, term=6,<br/>lastLogIndex=42, lastLogTerm=5

    C->>A: requestVote(VoteRequest)
    C->>B: requestVote(VoteRequest) [并发]

    Note over A: 检查：<br/>1. serverId 在配置中？<br/>2. request.term >= currentTerm?<br/>3. votedFor==0?<br/>4. 日志够新？<br/>→ 全满足: stepDown(6), votedFor=1
    A-->>C: VoteResponse{granted=true, term=6}

    Note over C: VoteResponseCallback.success()<br/>计票: voteGrantedNum=2 (self+A) > 3/2<br/>→ becomeLeader()

    Note over C: state → LEADER<br/>leaderId = selfId<br/>取消选举计时器<br/>启动心跳计时器 (500ms)
    C->>A: appendEntries(空entries=心跳)
    C->>B: appendEntries(空entries=心跳)
```

**requestVote 入参（VoteRequest）**

| 字段 | 来源 | 示例值 |
|------|------|--------|
| `server_id` | localServer.getServerId() | `1` |
| `term` | currentTerm（**已加1**） | `6` |
| `last_log_index` | raftLog.getLastLogIndex() | `42` |
| `last_log_term` | getLastLogTerm() | `5` |

**requestVote 出参（VoteResponse）**

| 字段 | 含义 | 授权条件 |
|------|------|---------|
| `granted` | 是否授票 | votedFor==0 且日志够新 |
| `term` | 响应方 term | currentTerm |

---

## 3. 场景二：Leader 选举（异常）

### 异常 A：收到更高 term 响应 → 立即降级

```mermaid
sequenceDiagram
    participant C as Candidate (term=6)
    participant A as Peer A (term=8)

    C->>A: requestVote{term=6}
    A-->>C: VoteResponse{granted=false, term=8}

    Note over C: VoteResponseCallback.success()<br/>response.term(8) > currentTerm(6)<br/>→ stepDown(8)
    Note over C: currentTerm=8, votedFor=0<br/>leaderId=0, state=FOLLOWER<br/>取消心跳，重置选举计时器
```

### 异常 B：选票分裂（Split Vote）→ 超时重试

```mermaid
sequenceDiagram
    participant C1 as Candidate-1 (term=6)
    participant C2 as Candidate-2 (term=6)
    participant A as Peer A

    C1->>A: requestVote{term=6, serverId=1}
    A-->>C1: VoteResponse{granted=true, term=6}
    Note over A: votedFor=1

    C2->>A: requestVote{term=6, serverId=2}
    Note over A: votedFor=1 != 0 → 拒绝
    A-->>C2: VoteResponse{granted=false, term=6}

    Note over C1: 未收到多数票（只有自己+A=2，但B也是Candidate）
    Note over C2: 未收到多数票

    Note over C1,C2: 两者都等待随机超时后<br/>重新触发 startPreVote()<br/>（term++ 再次竞选）
```

**stepDown 入参/出参**

| 操作 | 条件 | 副作用 |
|------|------|--------|
| 入参：newTerm | newTerm >= currentTerm | |
| 出参：无 | | currentTerm=newTerm, votedFor=0, leaderId=0, state=FOLLOWER, 重置选举计时器 |

### 异常 C：RPC 超时/失败

```
preVote/requestVote RPC 失败时：
  PreVoteResponseCallback.fail()  → peer.setVoteGranted(false)，记录日志，不重试
  VoteResponseCallback.fail()     → peer.setVoteGranted(false)，记录日志，不重试

影响：该 peer 计为反对票，如果多数派票数仍满足，照样晋级；
     否则等待下次选举超时重试整个流程。
```

---

## 4. 场景三：日志复制（正常）

**触发条件**：Client 调用 `replicate()` 或 Leader 发送心跳

```mermaid
sequenceDiagram
    participant Client
    participant L as Leader (Node-1)
    participant F1 as Follower-2
    participant F2 as Follower-3

    Client->>L: set("key", "value")

    Note over L: replicate(data, ENTRY_TYPE_DATA)
    Note over L: 加锁检查 state==LEADER
    Note over L: 构造 LogEntry{term=6, index=43, data}
    Note over L: raftLog.append([entry]) → newLastLogIndex=43
    Note over L: 并发提交 appendEntries 任务

    par 并发发送
        L->>F1: AppendEntriesRequest
        Note right of L: 入参: serverId=1,term=6,<br/>prevLogIndex=42,prevLogTerm=5,<br/>entries=[{43,term=6,data}],<br/>commitIndex=42
    and
        L->>F2: AppendEntriesRequest [同上]
    end

    Note over F1: appendEntries() 处理：<br/>1. term检查OK<br/>2. stepDown(6)→保持FOLLOWER<br/>3. prevLogIndex=42存在且term=5 ✓<br/>4. 写入 entries[43]<br/>5. advanceCommitIndex(request)→commitIndex=42<br/>6. 应用状态机
    F1-->>L: AppendEntriesResponse{RES_CODE_SUCCESS, term=6, lastLogIndex=43}

    Note over F2: 同上
    F2-->>L: AppendEntriesResponse{RES_CODE_SUCCESS, term=6, lastLogIndex=43}

    Note over L: 处理 F1 响应:<br/>peer.matchIndex=43, peer.nextIndex=44
    Note over L: advanceCommitIndex():<br/>matchIndexes=[43(F1), 43(F2), 43(L)]<br/>排序后 quorum(中位数)=43<br/>entry[43].term==currentTerm(6) ✓<br/>commitIndex: 42→43<br/>apply(data) → stateMachine<br/>lastAppliedIndex=43<br/>commitIndexCondition.signalAll()

    Note over L: replicate() 等待唤醒:<br/>lastAppliedIndex(43) >= newLastLogIndex(43)<br/>→ return true
    L-->>Client: success=true
```

**appendEntries 入参（AppendEntriesRequest）**

| 字段 | 来源 | 示例值 |
|------|------|--------|
| `server_id` | localServer.getServerId() | `1` |
| `term` | currentTerm | `6` |
| `prev_log_index` | peer.nextIndex - 1 | `42` |
| `prev_log_term` | raftLog.getEntryTerm(42) | `5` |
| `entries` | 从 nextIndex 起打包，最多 maxLogEntriesPerRequest 条 | `[{index=43,term=6,data=...}]` |
| `commit_index` | min(commitIndex, prevLogIndex + numEntries) | `42` |

**appendEntries 出参（AppendEntriesResponse）**

| 字段 | 成功时 | 失败时 |
|------|--------|--------|
| `res_code` | `RES_CODE_SUCCESS` | `RES_CODE_FAIL` |
| `term` | currentTerm | currentTerm |
| `last_log_index` | 最新日志 index | 冲突时返回回退建议 |

**advanceCommitIndex 逻辑（Leader 侧）**

```
matchIndexes = [F1.matchIndex, F2.matchIndex, ..., myLastLogIndex]
排序后取 matchIndexes[peerNum/2]  ← 中位数 = quorum
条件：entry[quorum].term == currentTerm（防止提交上一 term 的日志）
若满足：commitIndex = quorum，apply [oldCommit+1..newCommit]
```

---

## 5. 场景四：日志复制（异常——日志冲突）

### 异常 A：Follower 日志有 gap（prevLogIndex 超出 follower 的 lastLogIndex）

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower (lastLogIndex=40)

    L->>F: AppendEntries{prevLogIndex=42, entries=[43]}

    Note over F: prevLogIndex(42) > lastLogIndex(40)<br/>→ "would leave gap"<br/>→ return RES_CODE_FAIL, lastLogIndex=40

    F-->>L: AppendEntriesResponse{FAIL, lastLogIndex=40}

    Note over L: peer.nextIndex = response.lastLogIndex+1 = 41<br/>下次心跳时重新 appendEntries

    L->>F: AppendEntries{prevLogIndex=40, entries=[41,42,43]}
    Note over F: prevLogIndex(40)存在且term匹配 ✓<br/>写入41,42,43
    F-->>L: AppendEntriesResponse{SUCCESS, lastLogIndex=43}
```

### 异常 B：Follower 日志有冲突（同 index 不同 term）

```mermaid
sequenceDiagram
    participant L as Leader (term=6)
    participant F as Follower (index=43 term=4, 旧leader写的)

    L->>F: AppendEntries{prevLogIndex=42, prevLogTerm=5, entries=[{43,term=6}]}

    Note over F: prevLogIndex=42 存在，term=5 ✓<br/>处理 entries:<br/>  index=43: raftLog.getEntryTerm(43)=4 ≠ entry.term=6<br/>  → truncateSuffix(42)  ← 截断 index≥43 的旧日志<br/>  → 追加新的 entry{43,term=6}<br/>→ SUCCESS

    F-->>L: AppendEntriesResponse{SUCCESS, lastLogIndex=43}

    Note over L: matchIndex=43, nextIndex=44
```

### 异常 C：prevLogTerm 不匹配 → 回退 nextIndex

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower

    L->>F: AppendEntries{prevLogIndex=42, prevLogTerm=5}

    Note over F: raftLog.getEntryTerm(42) = 3 ≠ 5<br/>→ RES_CODE_FAIL<br/>→ lastLogIndex = prevLogIndex-1 = 41

    F-->>L: AppendEntriesResponse{FAIL, lastLogIndex=41}

    Note over L: peer.nextIndex = 41+1 = 42<br/>继续回退直到找到一致点
```

---

## 6. 场景五：心跳保活（正常）

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower

    Note over L: resetHeartbeatTimer() 每 500ms 触发<br/>startNewHeartbeat() → appendEntries(peer)

    L->>F: AppendEntriesRequest{term=6, prevLogIndex=43, entries=[], commitIndex=43}

    Note over F: entriesCount==0 → 心跳<br/>stepDown(6) 重置选举计时器<br/>advanceCommitIndex(request)<br/>→ return SUCCESS

    F-->>L: AppendEntriesResponse{SUCCESS, term=6, lastLogIndex=43}

    Note over L: response.term <= currentTerm ✓<br/>matchIndex=43, nextIndex=44
```

**心跳的核心作用**：
- Follower 收到后调用 `stepDown()` → 重置选举计时器（防止触发不必要的选举）
- 同步 commitIndex 给 Follower → Follower 提交已复制的日志

---

## 7. 场景六：快照生成（正常）

**触发条件**：定时器每 `snapshotPeriodSeconds`（默认3600s）触发，且 `raftLog.getTotalSize() >= snapshotMinLogSize`

```mermaid
flowchart TD
    A([定时器触发 takeSnapshot]) --> B{isInstallSnapshot?}
    B -- 是 --> Z([跳过])
    B -- 否 --> C{isTakeSnapshot.CAS false→true}
    C -- 失败 --> Z
    C -- 成功 --> D{raftLog.totalSize >= snapshotMinLogSize?}
    D -- 否 --> E([释放标志，返回])
    D -- 是 --> F{lastAppliedIndex > snapshot.lastIncludedIndex?}
    F -- 否 --> E
    F -- 是 --> G[获取 localLastAppliedIndex<br/>lastAppliedTerm<br/>localConfiguration]

    G --> H[创建临时目录<br/>snapshot.tmp/]
    H --> I[snapshot.updateMetaData 写入<br/>lastIncludedIndex=localLastAppliedIndex<br/>lastIncludedTerm<br/>configuration]
    I --> J[stateMachine.writeSnapshot<br/>snapshot.tmp/data/]
    J --> K{写入成功?}
    K -- 否 --> L([记录错误])
    K -- 是 --> M[原子移动<br/>snapshot.tmp → snapshot/]
    M --> N[snapshot.reload 重新加载元数据]
    N --> O[raftLog.truncatePrefix<br/>lastSnapshotIndex+1<br/>删除旧日志段]
    O --> P[isTakeSnapshot → false]
    P --> Q([快照完成])
```

**takeSnapshot 关键参数**

| 参数 | 值 | 说明 |
|------|-----|------|
| 触发阈值 | `snapshotMinLogSize`（默认1GB）| 日志总字节数 |
| 快照时间点 | `lastAppliedIndex` | 已应用到状态机的最新 index |
| 元数据写入 | `lastIncludedIndex`, `lastIncludedTerm`, `configuration` | 恢复时用 |
| 日志截断 | `truncatePrefix(lastSnapshotIndex + 1)` | 删除 index <= lastSnapshotIndex 的段 |

---

## 8. 场景七：快照安装（Follower 日志落后太多）

**触发条件**：`peer.nextIndex < raftLog.firstLogIndex`（Leader 已经把需要的日志删除了）

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower (落后很多)

    Note over L: appendEntries(peer) 时发现<br/>peer.nextIndex(10) < firstLogIndex(100)<br/>isNeedInstallSnapshot = true<br/>→ 调用 installSnapshot(peer)

    loop 分块发送（maxSnapshotBytesPerRequest=512KB/块）
        Note over L: buildInstallSnapshotRequest()<br/>读取 snapshot/data/ 下的文件<br/>按 offset 切块

        L->>F: InstallSnapshotRequest{<br/>  serverId=1, term=6,<br/>  snapshotMetaData={lastIncludedIndex=99,<br/>    lastIncludedTerm=5, configuration},<br/>  fileName="rocksdb_data/MANIFEST",<br/>  offset=0, data=[...512KB...],<br/>  isFirst=true, isLast=false<br/>}

        Note over F: isFirst=true:<br/>  创建 snapshot.tmp/ 目录<br/>  写入 metadata
        Note over F: 写数据到 snapshot.tmp/data/fileName 的 offset 处

        F-->>L: InstallSnapshotResponse{SUCCESS, term=6}

        L->>F: InstallSnapshotRequest{fileName="...", offset=512K, isFirst=false, isLast=false}
        F-->>L: InstallSnapshotResponse{SUCCESS}

        L->>F: InstallSnapshotRequest{..., isFirst=false, isLast=true}
        Note over F: isLast=true:<br/>  原子移动 snapshot.tmp → snapshot/<br/>  stateMachine.readSnapshot(snapshot/data/)<br/>  snapshot.reload()<br/>  raftLog.truncatePrefix(lastSnapshotIndex+1)<br/>  isInstallSnapshot = false

        F-->>L: InstallSnapshotResponse{SUCCESS, term=6}
    end

    Note over L: installSnapshot 完成:<br/>peer.nextIndex = lastIncludedIndex+1 = 100<br/>继续正常 appendEntries 从 100 开始
```

**InstallSnapshotRequest 入参**

| 字段 | 说明 | 示例 |
|------|------|------|
| `server_id` | Leader id | `1` |
| `term` | Leader 当前 term | `6` |
| `snapshot_meta_data` | {lastIncludedIndex, lastIncludedTerm, configuration} 只在 isFirst=true 时填充 | |
| `file_name` | 相对于 snapshot/data/ 的路径 | `"rocksdb_data/MANIFEST"` |
| `offset` | 文件内字节偏移 | `0`, `524288`, ... |
| `data` | 文件数据块 | `byte[512*1024]` |
| `is_first` | 是否第一个包（需清理旧 tmp 目录） | `true/false` |
| `is_last` | 是否最后一个包（触发 apply） | `true/false` |

**异常：快照安装中途失败**

```
Leader: response == null 或 resCode != SUCCESS
  → isSuccess = false → break 循环
  → closeSnapshotDataFiles
  → isInstallSnapshot.set(false)
  → installSnapshot 返回 false
  → appendEntries 直接 return（本次心跳放弃）
  → 下次心跳重新尝试

Follower: IO 异常
  → catch(IOException) → 记录日志
  → responseBuilder.setResCode(FAIL)
  → isInstallSnapshot 在 isLast=true 时才清除（保证幂等）
```

---

## 9. 场景八：客户端写请求完整链路

```mermaid
sequenceDiagram
    participant Client
    participant ES as ExampleServiceImpl
    participant L as RaftNode (Leader)
    participant F1 as Follower-2
    participant F2 as Follower-3
    participant SM as StateMachine(RocksDB)

    Client->>ES: set(SetRequest{key="x", value="1"})

    Note over ES: 检查 raftNode.getLeaderId()<br/>若非 leader → 转发给 leader 或报错
    ES->>L: replicate(data=SetRequest.bytes, ENTRY_TYPE_DATA)

    Note over L: 加锁<br/>state==LEADER ✓<br/>LogEntry{term=6, index=43, data=SetRequest.bytes}<br/>raftLog.append([entry])<br/>newLastLogIndex=43

    par 并发复制
        L->>F1: AppendEntries{prevLogIndex=42, entries=[43], commitIndex=42}
        L->>F2: AppendEntries{prevLogIndex=42, entries=[43], commitIndex=42}
    end

    F1-->>L: SUCCESS, lastLogIndex=43
    F2-->>L: SUCCESS, lastLogIndex=43

    Note over L: advanceCommitIndex()<br/>quorum matchIndex=43<br/>entry[43].term==6==currentTerm ✓<br/>commitIndex: 42→43

    L->>SM: stateMachine.apply(SetRequest.bytes)
    Note over SM: 解析 SetRequest<br/>rocksDB.put("x", "1")

    Note over L: lastAppliedIndex=43<br/>commitIndexCondition.signalAll()
    Note over L: replicate() 被唤醒<br/>lastAppliedIndex(43) >= newLastLogIndex(43)<br/>return true

    L-->>ES: true
    ES-->>Client: SetResponse{success=true}
```

**asyncWrite 模式差异**

```
asyncWrite=false（默认，同步写）：
  replicate() 等待 commitIndexCondition 被 signalAll()
  等待超时 maxAwaitTimeout（默认3000ms）
  超时后若 lastAppliedIndex < newLastLogIndex → return false

asyncWrite=true（异步写）：
  leader 本地写日志成功后立即 return true
  不等待 Follower 确认（牺牲强一致性换取低延迟）
```

**非 Leader 节点的写请求处理**

```
ExampleServiceImpl.set()：
  if (raftNode.getLeaderId() <= 0) → return error "no leader"
  if (raftNode.getLeaderId() != localServer.getServerId())
      → 通过 RPC 代理转发给 leader 节点
```

---

## 10. 场景九：节点崩溃恢复

```mermaid
flowchart TD
    A([节点重启 RaftNode 构造函数]) --> B[SegmentedLog 加载日志元数据<br/>currentTerm, votedFor, firstLogIndex]
    B --> C[Snapshot.reload() 加载快照元数据<br/>lastIncludedIndex, lastIncludedTerm, configuration]
    C --> D[commitIndex = max(snapshot.lastIncludedIndex, 0)]
    D --> E{snapshot.lastIncludedIndex > 0 &&<br/>raftLog.firstLogIndex <= snapshot.lastIncludedIndex?}
    E -- 是 --> F[raftLog.truncatePrefix(lastIncludedIndex+1)<br/>丢弃已包含在快照中的日志]
    E -- 否 --> G
    F --> G[从快照配置恢复 configuration]
    G --> H[stateMachine.readSnapshot(snapshot/data/)]
    H --> I[从 lastIncludedIndex+1 到 commitIndex<br/>重放日志到状态机]
    I --> J[lastAppliedIndex = commitIndex]
    J --> K[init() 初始化线程池<br/>启动快照定时器<br/>resetElectionTimer()]
    K --> L([节点就绪，以 FOLLOWER 身份参与集群])
```

**崩溃场景分析**

| 崩溃时机 | 崩溃影响 | 恢复方式 |
|----------|----------|---------|
| 日志写入前 | 条目未持久化 | 重启后无该条目，Leader 重发 |
| 日志写入后未复制 | 孤立日志条目 | 若 Leader 崩溃：新 Leader 可能覆盖；若 Follower 崩溃：Leader 重发并 truncateSuffix |
| 日志已复制未提交 | 多数 Follower 有该条目 | 新 Leader 选出后提交该条目（日志够新才能当选） |
| 日志已提交 | commitIndex 已更新 | 重启后通过 raftLog 或 snapshot 恢复状态机 |
| 快照写入中途 | snapshot.tmp 存在 | 下次快照重新生成（tmp 目录被覆盖） |

---

## 11. 消息格式速查

### VoteRequest / VoteResponse
```protobuf
message VoteRequest {
    int32 server_id = 1;    // 发起方 id
    int64 term = 2;         // 任期（preVote 不加1，vote 已加1）
    int64 last_log_term = 3;
    int64 last_log_index = 4;
}
message VoteResponse {
    int64 term = 1;
    bool granted = 2;
}
```

### AppendEntriesRequest / Response
```protobuf
message AppendEntriesRequest {
    int32 server_id = 1;
    int64 term = 2;
    int64 prev_log_index = 3;
    int64 prev_log_term = 4;
    repeated LogEntry entries = 5;  // 空 = 心跳
    int64 commit_index = 6;
}
message AppendEntriesResponse {
    ResCode res_code = 1;    // SUCCESS or FAIL
    int64 term = 2;
    int64 last_log_index = 3; // 失败时用于回退 nextIndex
}
```

### InstallSnapshotRequest / Response
```protobuf
message InstallSnapshotRequest {
    int32 server_id = 1;
    int64 term = 2;
    SnapshotMetaData snapshot_meta_data = 3; // 只在 isFirst=true 时填
    string file_name = 4;
    int64 offset = 5;
    bytes data = 6;
    bool is_first = 7;
    bool is_last = 8;
}
message InstallSnapshotResponse {
    ResCode res_code = 1;
    int64 term = 2;
}
```

---

## 异常处理总结表

| 异常场景 | 检测点 | 处理逻辑 | 入参/出参变化 |
|----------|--------|----------|-------------|
| 收到更高 term 的 RPC | 任何 RPC handler | `stepDown(newTerm)` | currentTerm↑, state→FOLLOWER |
| AppendEntries 有 gap | prevLogIndex > lastLogIndex | 返回 FAIL，lastLogIndex=当前值 | peer.nextIndex = lastLogIndex+1 |
| prevLogTerm 不匹配 | raftLog.getEntryTerm(prevLogIndex)!=prevLogTerm | 返回 FAIL，lastLogIndex=prevLogIndex-1 | peer.nextIndex 回退 |
| 日志条目 term 冲突 | entry.term != 本地同 index 的 term | truncateSuffix(index-1) 后追加 | 本地日志截断重写 |
| Follower 落后太多 | peer.nextIndex < firstLogIndex | installSnapshot 分块传送 | peer.nextIndex = lastSnapshotIndex+1 |
| 选票分裂 | voteGrantedNum <= total/2 | 等待超时，下轮选举 term++ 重试 | currentTerm++ |
| 网络断开后恢复 | Pre-vote 阻止 term 飙升 | preVote 不加 term，Peer 以当前 term 比较 | currentTerm 不膨胀 |
| Leader 脑裂 | 收到两个 leader 的 AppendEntries | stepDown(term+1) 强制双方降级 | 两个 leader 都变 FOLLOWER |
| 快照生成与安装并发 | isInstallSnapshot / isTakeSnapshot | 互斥标志，后来者直接返回 | 操作被跳过 |

---

*文档基于 raft-java 源码 `RaftNode.java` + `RaftConsensusServiceImpl.java` 生成*
