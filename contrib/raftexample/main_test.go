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
	"flag"
	"strings"
	"testing"

	"go.etcd.io/etcd/raft/raftpb"
)

func Test_1(t *testing.T) {
	cluster := flag.String("cluster", "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 12380, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	//当kvstore中收到配置添加请求时会向proposeC通道发送kv数据，在raft中会得到proposeC通道的事件进行处理
	proposeC := make(chan string)
	defer close(proposeC)
	//当kvstore中收到集群节点变更请求时会向confChangeC通道发送集群变更数据，在raft中会得到confChangeC通道的事件进行处理
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	//当raft中数据可以提交时会向commitC通道发送消息，这样kvstore就可以监听该通道消息，当收到提交消息时会修改kvstore内存中的值
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)
	//直到snapshotterReady通道有数据了，即snapshot可用了，才可以创建kvstore实例
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	//启动一个http服务
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
