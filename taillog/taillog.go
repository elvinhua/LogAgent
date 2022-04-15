/*
 * @title:
 * @Description:
 * @version:
 * @Author: Guoyh
 * @Date: 2022-04-07 11:35:34
 * @LastEditors: Guoyh
 * @LastEditTime: 2022-04-07 11:35:37
 */

package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"ibuyd.com/Guoyh/logagent/kafka"
)

//var tailObj *tail.Tail

// 一个具体日志收集的任务
type tailTask struct {
	path       string
	topic      string
	instance   *tail.Tail
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewTailTask 根据传入的path topic创建tailTask结构体
func NewTailTask(path, topic string) (tailObj *tailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &tailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	// 初始化tail
	tailObj.init()
	return
}

// 初始化tail
func (t *tailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 是否重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件的哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	}
	var err error
	// 根据不同的path创建tail
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file faild err:", err)
	}
	// 初始化tail后，开启goroutine 将数据发送到kafka
	go t.run()
}

// 循环跑配置信息获取某个topic的日志信息
func (t *tailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail tast:%s_%s 结束了……\n", t.path, t.topic)
			return
		case line := <-t.instance.Lines:
			// 将topic下读取的每条数据，发送到kafka设置好的通道中
			kafka.SendToChan(t.topic, line.Text)
		}
	}
}
