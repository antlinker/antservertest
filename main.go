package main

import (
	"fmt"
	"os"

	"net/http"
	_ "net/http/pprof"

	"github.com/antlinker/alog"
	"github.com/antlinker/antservertest/publish"
	"github.com/codegangsta/cli"
)

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("0.0.0.0:9090", nil))
	}()
	alog.RegisterAlog("conf/log.yaml")
	alog.SetLogTag("___+++___")
	app := cli.NewApp()
	app.Name = "mqttpersonal"
	app.Author = "Lyric"
	app.Version = "0.1.0"
	app.Usage = "MQTT单人聊天测试"

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "ExecNum, en",
			Value: 10,
			Usage: "执行次数",
		},
		cli.IntFlag{
			Name:  "Interval, i",
			Value: 5,
			Usage: "发布间隔(单位:秒)",
		},
		cli.IntFlag{
			Name:  "UserInterval, ui",
			Value: 1,
			Usage: "好友发包间隔（单位毫秒）",
		},
		cli.IntFlag{
			Name:  "AutoReconnect, ar",
			Value: 1,
			Usage: "客户端断开连接后执行自动重连(默认为1，0表示不重连)",
		},
		cli.IntFlag{
			Name:  "DisconnectScale, ds",
			Value: 0,
			Usage: "发送完成之后，需要断开客户端的比例(默认为0，不断开)",
		},

		cli.StringFlag{
			Name:  "Address, addr",
			Value: "tcp://113.59.225.149:1883",
			Usage: "MQTT Address tcp://test.antservercampus.link:1883",
		},

		cli.StringFlag{
			Name:  "AuthHost, auth",
			Value: "https://test.antservercampus.link:8803",
			Usage: "登录服务器网址 例如http://ip:port",
		},
		cli.IntFlag{
			Name:  "KeepAlive, alive",
			Value: 60,
			Usage: "MQTT KeepAlive",
		},
		cli.IntFlag{
			Name:  "ClientCnt, cc",
			Value: 10,
			Usage: "用户数量,最大1000,最小1",
		},
		cli.IntFlag{
			Name:  "StartCode, sc",
			Value: 0,
			Usage: "用户最小序号,最小0",
		},
		cli.BoolFlag{
			Name:  "CleanSession, cs",
			Usage: "MQTT CleanSession",
		},
	}
	app.Action = func(ctx *cli.Context) {
		cfg := &publish.Config{
			ExecNum:      ctx.Int("ExecNum"),
			Interval:     ctx.Int("Interval"),
			UserInterval: ctx.Int("UserInterval"),
			Address:      ctx.String("Address"),
			Auth:         ctx.String("AuthHost"),
			CleanSession: ctx.Bool("CleanSession"),
			KeepAlive:    ctx.Int("KeepAlive"),
			ClientCnt:    ctx.Int("ClientCnt"),
			StartCode:    ctx.Int("StartCode"),
		}
		if ctx.Int("AutoReconnect") == 1 {
			cfg.AutoReconnect = true
		}
		publish.Pub(cfg)
	}

	app.Run(os.Args)
}
