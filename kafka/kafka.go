package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// 专门往kafka写日志的模块

type logData struct {
	topic string
	data  string
}

var (
	// 声明一个全局的链接kafka的生产者client
	client      sarama.SyncProducer
	logDataChan chan *logData
)

//Init 初始化kafka
func Init(addr []string, maxSize int) (err error) {
	// 配置kafka
	config := sarama.NewConfig()
	// 配置生产者的ACK 即：0、1、all （当前选择leader 和 follow都确认 all）
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要
	// 配置kafka分区的Partition规则 未指定partition 也未传key所以为轮询随机指定
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	//	链接kafka
	client, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		fmt.Println("producer closed err:", err)
		return
	}
	logDataChan = make(chan *logData, maxSize)
	go sendToKafka()
	return
}

// SendToChan 将tail传入的信息，构建logData结构体
func SendToChan(topic, data string) {
	ld := &logData{
		topic: topic,
		data:  data,
	}
	// 发送达到配置好的logDataChan 通道中
	logDataChan <- ld
}

func sendToKafka() {
	for {
		select {
		case ld := <-logDataChan:
			// 构造一个消息
			// 发送消息传参是一个指针类型，所以这里要用取地址符（&）
			msg := &sarama.ProducerMessage{}
			// 配置topic
			msg.Topic = ld.topic
			// 构造的ProducerMessage Value是其内部的Encode类型所以将消息转为StringEncoder存放
			msg.Value = sarama.StringEncoder(ld.data)
			//	发送消息
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed, err:", err)
				return
			}
			fmt.Printf("pid:%v offset:%v\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 50)
		}

	}
}
