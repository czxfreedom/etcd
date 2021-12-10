// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v) 建议的消息(k,v) 业务数据变更 走这个通道
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes 集群节点扩容,新增 走这个通道
	commitC     chan<- *string           // entries committed to log (k,v) 状态机处理后,给上层返回已经commit的消息,都在这个通道
	errorC      chan<- error             // errors from raft session  错误处理 跟业务层打交道

	id          int                    // client ID for raft session  //raft会话的客户端ID
	peers       []string               // raft peer URLs             raft对等URL  raft集群配置
	join        bool                   // node is joining an existing cluster     //节点正在加入现有群集
	waldir      string                 // path to WAL directory    wal目录路径
	snapdir     string                 // path to snapshot directory  快照目录路径
	getSnapshot func() ([]byte, error) //业务层定义的 打快照的方法
	lastIndex   uint64                 // index of log at start     开始时的日志索引

	confState     raftpb.ConfState
	snapshotIndex uint64 //最近一次快照记录的日志index,快照之前是可以被安全删除的,这个赋值有俩种情况,1,触发一次快照,2.从持久化中加载
	appliedIndex  uint64 //这个之前一定是apply过的日志,这个是日志索引

	// raft backing for the commit/error channel  提交/错误通道的raft备份
	node        raft.Node //raft状态机
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready  快照机准备就绪时发出信号
	snapCount        uint64
	transport        *rafthttp.Transport
	stopc            chan struct{} // signals proposal channel closed
	httpstopc        chan struct{} // signals http server to shutdown
	httpdonec        chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.

//newRaftNode启动raft实例并返回提交的日志条目
// 主要完成了raftNode的初始化
// 使用上层模块传入的配置信息来创建raftNode实例，同时创建commitC 通道和errorC通道返回给上层模块使用
// 上层的应用通过这几个channel就能和raftNode进行交互
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)
	// channel，主要传输Entry记录
	// raftNode会将etcd-raft模块返回的待应用Entry记录（封装在 Ready实例中〉写入commitC通道，另一方面，
	//kvstore会从commitC通道中读取这些待应用的 Entry 记录井保存其中的键值对信息。
	rc := &raftNode{
		proposeC:         proposeC,
		confChangeC:      confChangeC,
		commitC:          commitC,
		errorC:           errorC,
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("raftexample-%d", id),
		snapdir:          fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot:      getSnapshot,
		snapCount:        defaultSnapshotCount,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	// 启动一个goroutine,完成剩余的初始化工作
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

//1、创建 Snapshotter，并将该 Snapshotter 实例返回给上层模块；
//2、创建 WAL 实例，然后加载快照并回放 WAL 日志；
//3、创建 raft.Config 实例，其中包含了启动 etcd-raft 模块的所有配置；
//4、初始化底层 etcd-raft 模块，得到 node 实例；
//5、创建 Transport 实例，该实例负责集群中各个节点之间的网络通信，其具体实现在 raft-http 包中；
//6、建立与集群中其他节点的网络连接；
//7、启动网络组件，其中会监听当前节点与集群中其他节点之间的网络连接，并进行节点之间的消息读写；
//8、启动两个后台的 goroutine，它们主要工作是处理上层模块与底层 etcd-raft 模块的交互，
//但处理的具体内容不同，后面会详细介绍这两个 goroutine 的处理流程。
func (rc *raftNode) startRaft() {
	//如果snapshot目录不存在，则创建snapshot目录，目录命名规则:raftexample-id-snap
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter
	// 创建 WAL 实例，然后加载快照并回放 WAL 日志
	oldwal := wal.Exist(rc.waldir)
	// raftNode.replayWAL() 方法首先会读取快照数据，
	//在快照数据中记录了该快照包含的最后一条 Entry 记录的 Term 值 和 索引值。
	//然后根据 Term 值 和 索引值确定读取 WAL 日志文件的位置， 并进行日志记录的读取。

	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	//raft.Config 实例
	c := &raft.Config{
		ID: uint64(rc.id),
		//选举定时器
		ElectionTick: 10,
		//心跳定时器
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	// 初始化底层的 etcd-raft 模块，这里会根据 WAL 日志的回放情况，
	// 判断当前节点是首次启动还是重新启动
	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	// 启动网络服务相关组件
	rc.transport.Start()
	// 建立与集群中其他各个节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
	// 跟其他raft节点进行网络通信
	go rc.serveRaft()
	//channel 消息的处理，真正核⼼的定制，处理 proposeC，confChangeC，commitC，errorC 这四个通道
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

// 会单独启动一个后台 goroutine来负责上层模块 传递给 etcd-raft 模块的数据，
// 主要 处理前面介绍的 proposeC、 confChangeC 两个通道
//处理上层应用与底层etcd-raft模块的交互
func (rc *raftNode) serveChannels() {
	//获取快照数据和快照的元数据
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	// 创建一个每隔 lOOms 触发一次的定时器，那么在逻辑上，lOOms 即是 etcd-raft 组件的最小时间单位 ，
	// 该定时器每触发一次，则逻辑时钟推进一次
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 单独启动一个 goroutine 负责将 proposeC、 confChangeC 上接收到
	// 的数据传递给 etcd-raft 组件进行处理

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					// 发生异常将proposeC置空
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					// 阻塞直到消息被处理
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			// 收到上层应用通过 confChangeC远远传递过来的数据
			case cc, ok := <-rc.confChangeC:
				if !ok {
					// 如果发生异常将confChangeC置空
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// 关闭 stopc 通道，触发 rafeNode.stop() 方法的调用
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()
	// 处理 etcd-raft 模块返回给上层模块的数据及其他相关的操作
	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			// 上述 ticker 定时器触发一次
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		// 读取 node.readyc 通道
		// 该通道是 etcd-raft 组件与上层应用交互的主要channel之一
		// 其中传递的 Ready 实例也封装了很多信息
		case rd := <-rc.node.Ready():
			// 将当前 etcd raft 组件的状态信息，以及待持久化的 Entry 记录先记录到 WAL 日志文件中，
			// 即使之后宕机，这些信息也可以在节点下次启动时，通过前面回放 WAL 日志的方式进行恢复
			rc.wal.Save(rd.HardState, rd.Entries)
			// 检测到 etcd-raft 组件生成了新的快照数据
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 将新的快照数据写入快照文件中
				rc.saveSnap(rd.Snapshot)
				// 将新快照持久化到 raftStorage
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				// 通知上层应用加载新快照
				rc.publishSnapshot(rd.Snapshot)
			}
			// 将待持久化的 Entry 记录追加到 raftStorage 中完成持久化
			rc.raftStorage.Append(rd.Entries)
			// 将待发送的消息发送到指定节点
			rc.transport.Send(rd.Messages)
			// 将已提交、待应用的 Entry 记录应用到上层应用的状态机中
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			// 随着节点的运行， WAL 日志量和 raftLog.storage 中的 Entry 记录会不断增加 ，
			// 所以节点每处理 10000 条(默认值) Entry 记录，就会触发一次创建快照的过程，
			// 同时 WAL 会释放一些日志文件的句柄，raftLog.storage 也会压缩其保存的 Entry 记录
			rc.maybeTriggerSnapshot()
			// 上层应用处理完该 Ready 实例，通知 etcd-raft 纽件准备返回下一个 Ready 实例
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	//解析出地址
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}
	//定制一个listener
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}
	//创建一个http handler 用于走raft数据流
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
