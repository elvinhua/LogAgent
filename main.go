package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"ibuyd.com/Guoyh/logagent/conf"
	"ibuyd.com/Guoyh/logagent/etcd"
	"ibuyd.com/Guoyh/logagent/kafka"
	"ibuyd.com/Guoyh/logagent/taillog"
	"ibuyd.com/Guoyh/logagent/utils"
	"sync"
	"time"
)

var (
	cfg = new(conf.AppConf)
)

// logAgent 入口文件
func main() {
	// 加载配置信息
	err := ini.MapTo(cfg, "./conf/config.ini")
	//cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		fmt.Println("Fail to read file err:", err)
	}
	// 1.初始化kafka链接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Println("init kafka faild,err:", err)
		return
	}
	fmt.Println("init kafka success")

	// 2.初始化etcd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("init etcd failed err:", err)
		return
	}
	fmt.Println("init etcd success")

	// 获取当前服务器的ip 并拉去对应的配置信息
	ipStr, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	//etcdConfKey := cfg.EtcdConf.Key
	fmt.Println(etcdConfKey)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Println("etcd.GetConf failed err:", err)
		return
	}
	fmt.Println("get conf from etcd success data:", logEntryConf)
	for i, entry := range logEntryConf {
		fmt.Printf("index:%v value:%+ v\n", i, entry)
	}
	// 3.收集日志发往kafka
	// 3.1 初始化tail组
	taillog.Init(logEntryConf)
	// tail中对外暴露的通道 （taillog_mgr中init初始化make的无缓冲区通道 空的）
	newConfChan := taillog.NewConfChan()
	var wg sync.WaitGroup
	wg.Add(1)
	// 2.2 派一个哨兵 watch 监视etcd的信息变动（有变化及时通知logAgent 实现热加载）
	// 通过key获取新的配置信息，放入通道中
	go etcd.WatchConf(etcdConfKey, newConfChan)
	wg.Wait()
}
