package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var cli *clientv3.Client

type LogEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

// Init 初始化etcd
func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeout,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed err: %#v", err)
		return
	}
	return
}

// GetConf 从etcd中根据key获取配置项
func GetConf(key string) (logentry []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Println("get from etcd failed err:", err)
		return
	}
	for _, env := range resp.Kvs {
		err = json.Unmarshal(env.Value, &logentry)
		if err != nil {
			fmt.Println("unmarshal etcd value failed err:", err)
			return
		}
	}
	return
}

// 创建watch 哨兵 监视配置文件内容更新

func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	ch := cli.Watch(context.Background(), key)
	for wresp := range ch {
		for _, env := range wresp.Events {
			//	通知taillog.tskMgr
			var newConf []*LogEntry
			if env.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(env.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("json unmarshal faild err:", err)
					continue
				}
			}
			fmt.Printf("get new conf %v\n", newConf)
			// 将新的配置信息发送给 taillogMgr中设置的无缓冲区空通道中
			newConfCh <- newConf
		}
	}
}
