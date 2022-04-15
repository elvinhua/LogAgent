package taillog

import (
	"fmt"
	"ibuyd.com/Guoyh/logagent/etcd"
	"time"
)

var tskMgr *taillogMgr

type taillogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*tailTask
	newConfChan chan []*etcd.LogEntry
}

// Init 初始化tail组管理
func Init(logEntryConf []*etcd.LogEntry) {
	// 配置taillogMgr组管理结构体
	tskMgr = &taillogMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*tailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区通道，无数据保持阻塞
	}
	// 循环配置信息
	for _, logEntry := range logEntryConf {
		// 初始化每个配置信息，并记录有多少个tailTask
		tailtaskObj := NewTailTask(logEntry.Path, logEntry.Topic)
		// 记录取得的path 和 topic信息
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mk] = tailtaskObj
	}
	go tskMgr.run()
}

// 监听自己的newConfChan,有了新配置之后做处理
// 一直跑
func (t *taillogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, newC := range newConf {
				mk := fmt.Sprintf("%s_%s", newC.Path, newC.Topic)
				if _, ok := t.tskMap[mk]; ok {
					continue
				} else {
					tailObj := NewTailTask(newC.Path, newC.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			for _, c1 := range t.logEntry {
				isDel := true
				for _, c2 := range newConf {
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDel = false
						continue
					}
				}
				if isDel {
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[mk].cancelFunc()
				}
			}
			fmt.Println("新的配置来了", newConf)
		default:
			time.Sleep(time.Second)
		}

	}
}

// NewConfChan  向外暴露tskMgr的newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
