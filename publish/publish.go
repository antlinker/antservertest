package publish

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	MQTT "github.com/antlinker/go-mqtt/client"

	"github.com/antlinker/alog"
	"gopkg.in/mgo.v2"

	"github.com/antlinker/antservertest/config"
	"github.com/antlinker/antservertest/uuid"
	"github.com/antlinker/go-cmap"
)

type loginSuccess struct {
	UID        string `json:"UID"`
	RE         int    `json:"RE"`
	APPMQTTKEY string `json:"APPMQTTKEY"`
}
type loginRequest struct {
	UID          string `json:"UID"`
	Upswd        string `json:"UPASS"`
	Universityid string `json:"UNIVERSITYID"`
	Loginmode    int    `json:"LOGINMODE"` //登录的方式 1为手机号 + 密码；2为学校编号+学号+密码
	Deviceid     string `json:"DEVICEID"`
	Token        string `jo:"TOKEN"`
	Devicetype   string `json:"DEVICETYPE"`
	Logintype    string `json:"LOGINTYPE"`
	Channel      string `json:"REGCHANNEL"` //渠道
	Version      string `json:"VERSION"`
}

// createLoginString 创建登录报文请求
func createLoginString(uid string) *loginRequest {

	return &loginRequest{
		UID:          uid,
		Upswd:        "E10ADC3949BA59ABBE56E057F20F883E",
		Universityid: "16011",
		Loginmode:    1,
		Deviceid:     fmt.Sprintf("DevIDTest%s", string(uid[len(uid)-4:])),
		Token:        "jfewiow324jeo3",
		Devicetype:   "android",
		Channel:      "AB",
		Version:      "2.0.1",
		Logintype:    "manual",
	}

}

// Pub 执行Publish操作
func Pub(cfg *Config) {
	pub := &publish{
		cfg:             cfg,
		clientIndexData: make(map[string]int),
		clients:         cmap.NewConcurrencyMap(),
		execComplete:    make(chan bool, 1),
		end:             make(chan bool, 1),
		startTime:       time.Now(),
		clientCnt:       cfg.ClientCnt,
		startCode:       cfg.StartCode,
	}
	if cfg.StartCode < 0 {
		pub.startCode = 0
	}
	pub.mqttKeyURL = cfg.Auth + "/app/login"
	err := pub.Init()
	if err != nil {
		alog.Error(err)
		return
	}
	pub.ExecPublish()
	<-pub.end
	alog.Info("执行完成.")
	time.Sleep(time.Second * 2)
	os.Exit(0)
}

type publish struct {
	cfg        *Config
	session    *mgo.Session
	database   *mgo.Database
	clientData []config.ClientInfo

	clientIndexData map[string]int

	infoClientData  []config.ClientInfo
	infoClients     map[string]int
	clients         cmap.ConcurrencyMap
	publishID       int64
	execNum         int64
	execComplete    chan bool
	publishTotalNum int64
	publishNum      int64
	prePublishNum   int64
	maxPublishNum   int64
	arrPublishNum   []int64
	receiveTotalNum int64
	receiveNum      int64
	preReceiveNum   int64
	maxReceiveNum   int64
	arrReceiveNum   []int64
	end             chan bool
	startTime       time.Time

	subscribeNum      int64
	subscribeTotalNum int64
	mqttKeyURL        string
	clientCnt         int
	startCode         int
}

func (p *publish) Init() error {

	err := p.initData()
	if err != nil {
		return err
	}
	err = p.initConnection()
	if err != nil {
		return err
	}
	alog.Info("初始化操作完成.")
	calcTicker := time.NewTicker(time.Second * 1)
	go p.calcMaxAndMinPacketNum(calcTicker)
	go p.checkComplete()
	prTicker := time.NewTicker(time.Duration(p.cfg.Interval) * time.Second)
	go p.pubAndRecOutput(prTicker)
	return nil
}

func (p *publish) getMqttKey(uid string) string {
	var logResp loginSuccess

	postr(p.mqttKeyURL, createLoginString(uid), &logResp)
	alog.Info("logResp:", logResp)
	if logResp.RE == 0 {
		return logResp.APPMQTTKEY
	}
	return ""
}

/*
测试准备的1000个用户ID: DA0000001000 到 DA DA0000001999 ，密码都是123456

这些号，每个都有2个好友分别是 自己的尾号+1000 和 +2000
例如 DA0000001000 的好友有 DA0000002000 和 DA0000003000
     DA0000001678 的好友有 DA0000002678 和 DA0000003678

聊天的报文，发送到主题 S/U2U，其中AID的值必须每次都不同，其他的可以重复
以DA0000001000 发送聊天消息给 DA0000002000 为例子 ：
{
    "AID": "43724242",
    "MT": "MSG",
    "ST": "20140923092047",
    "FUID": "DA0000001000",
    "TUID": "DA0000002000",
    "CT": "wz",
    "VOICELEN": "0",
    "PICTYPE": "",
    "CONTENT": "helllo,world"
}

批量查询通知的报文，发送到主题 S/INFO，这个报文的AID可以重复：
{
    "MT": "INFOQUERYBYIDLIST",
    "AID": "0023AF33",
    "ST": "20140923092047",
    "UID": "DA0000001000",
    "IDLIST": [
        "100000000144",
        "100000000134",
        "100000000133",
        "100000000132",
        "100000000131",
        "100000000130",
        "100000000129",
        "100000000123",
        "100000000119",
        "100000000108"
    ]
}*/
func (p *publish) initData() error {
	alog.Info("开始客户端数据初始化...")
	p.clientData = make([]config.ClientInfo, 0)
	p.infoClientData = make([]config.ClientInfo, 0)
	p.createData()
	for i := 0; i < len(p.clientData); i++ {
		client := p.clientData[i]
		p.clientIndexData[client.ClientID] = i
	}
	alog.Info("客户端数据初始化完成.")
	return nil
}
func (p *publish) createData() {

	for i := 0; i < p.clientCnt; i++ {
		cid := fmt.Sprintf("DA000000%d", i+1000+p.startCode)
		cid1 := fmt.Sprintf("DA000000%d", i+p.startCode+2000)
		cid2 := fmt.Sprintf("DA000000%d", i+p.startCode+3000)
		info := config.ClientInfo{
			ClientID:  cid,
			Relations: []string{cid1, cid2},
			Weight:    1,
		}

		info1 := config.ClientInfo{
			ClientID:  cid1,
			Relations: []string{cid},
			Weight:    1,
		}
		info2 := config.ClientInfo{
			ClientID:  cid2,
			Relations: []string{cid},
			Weight:    1,
		}
		p.clientData = append(p.clientData, info)
		p.infoClientData = append(p.infoClientData, info1)
		p.infoClientData = append(p.infoClientData, info2)

	}
	type ClientInfo struct {
		ClientID  string   `bson:"clientid"`
		Relations []string `bson:"relations"`
		Weight    int      `bson:"weight"`
	}
}
func (p *publish) oneConnection(uid string) {
LB_MQTTKEY:
	key := p.getMqttKey(uid)
	if key == "" {
		alog.Debug("获取登录令牌失败!")
		time.Sleep(10 * time.Microsecond)

		goto LB_MQTTKEY
	}
	opts := MQTT.MqttOption{
		Addr:               p.cfg.Address,
		Clientid:           fmt.Sprintf("DevIDTest%s_%s", string(uid[len(uid)-4:]), uid),
		ReconnTimeInterval: 1,
		UserName:           uid,
		Password:           key,
	}
	if v := p.cfg.KeepAlive; v > 0 {
		opts.KeepAlive = uint16(v)
	}
	if v := p.cfg.CleanSession; v {
		opts.CleanSession = v
	}
	clientHandle := newHandleConnect(uid, p)
	cli, err := MQTT.CreateClient(opts)
	if err != nil {
		panic(fmt.Sprintf("创建mqttclient失败:%v", err))
	}
	cli.AddConnListener(clientHandle)
	cli.AddRecvPubListener(clientHandle)
	cli.AddPubListener(clientHandle)
	cli.AddSubListener(clientHandle)
LB_RECONNECT:

	if err = cli.Connect(); err != nil {
		alog.Errorf("客户端[%s]建立连接发生异常:%s", uid, err)
		time.Sleep(10 * time.Microsecond)
		goto LB_RECONNECT
	}
	topic := "C/" + uid
	topic2 := fmt.Sprintf("D/DevIDTest%s", string(uid[len(uid)-4:]))

	//LB_SUBSCRIBE:
	token, err := cli.Subscribes(MQTT.CreateSubFilter(topic, 1), MQTT.CreateSubFilter(topic2, 1))
	if err != nil {
		alog.Info("订阅失败．重新连接.")
		cli.Disconnect()

		time.Sleep(10 * time.Microsecond)
		goto LB_RECONNECT
	}
	token.Wait()
	if token.Err() != nil {
		alog.Info("订阅失败．重新订阅.")
		time.Sleep(10 * time.Microsecond)

		goto LB_RECONNECT
	}
	p.clients.Set(uid, cli)
}
func (p *publish) initConnection() error {
	alog.Info("开始建立MQTT数据连接初始化...")
	var wgroup sync.WaitGroup
	cl := len(p.clientData)
	wgroup.Add(cl * 3)
	alog.Infof("需要建立:%d个连接", cl)
	p.subscribeTotalNum += int64(cl)
	connsuccess := int32(0)
	for _, info := range p.clientData {
		go func(clientInfo config.ClientInfo) {
			defer wgroup.Done()
			p.oneConnection(clientInfo.ClientID)

			atomic.AddInt32(&connsuccess, 1)
			alog.Info(clientInfo.ClientID, "连接成功:", connsuccess, "剩余:", (cl*3)-int(connsuccess))
		}(info)
	}

	for _, info := range p.infoClientData {
		go func(clientInfo config.ClientInfo) {
			defer wgroup.Done()
			p.oneConnection(clientInfo.ClientID)
			atomic.AddInt32(&connsuccess, 1)
			alog.Info(clientInfo.ClientID, "连接成功", connsuccess, "剩余:", (cl*3)-int(connsuccess))
		}(info)
	}
	wgroup.Wait()
	alog.Info("MQTT数据连接初始化完成.")
	return nil
}

func (p *publish) checkComplete() {
	<-p.execComplete
	go func() {
		ticker := time.NewTicker(time.Millisecond * 500)
		for range ticker.C {
			if p.receiveNum >= p.receiveTotalNum {
				ticker.Stop()
				p.end <- true
			}
		}
	}()
}

func (p *publish) calcMaxAndMinPacketNum(ticker *time.Ticker) {
	for range ticker.C {
		pubNum := p.publishNum
		recNum := p.receiveNum
		pNum := pubNum - p.prePublishNum
		rNum := recNum - p.preReceiveNum
		if pNum > p.maxPublishNum {
			p.maxPublishNum = pNum
		}
		p.arrPublishNum = append(p.arrPublishNum, pNum)
		if rNum > p.maxReceiveNum {
			p.maxReceiveNum = rNum
		}
		p.arrReceiveNum = append(p.arrReceiveNum, rNum)
		p.prePublishNum = pubNum
		p.preReceiveNum = recNum
	}
}

func (p *publish) pubAndRecOutput(ticker *time.Ticker) {
	for ct := range ticker.C {
		currentSecond := float64(ct.Sub(p.startTime)) / float64(time.Second)
		var psNum int64
		arrPNum := p.arrPublishNum
		if len(arrPNum) == 0 {
			continue
		}
		for i := 0; i < len(arrPNum); i++ {
			psNum += arrPNum[i]
		}
		avgPNum := psNum / int64(len(arrPNum))
		var rsNum int64
		arrRNum := p.arrReceiveNum
		for i := 0; i < len(arrRNum); i++ {
			rsNum += arrRNum[i]
		}
		avgRNum := rsNum / int64(len(arrRNum))
		clientNum := p.clients.Len()
		output := `
总耗时                      %.2fs
执行次数                    %d
客户端数量                  %d
订阅数量                    %d
实际订阅量                  %d
应发包量                    %d
实际发包量                  %d
每秒平均的发包量            %d
每秒最大的发包量            %d
应收包量                    %d
实际收包量                  %d
每秒平均的接包量            %d
每秒最大的接包量            %d`

		fmt.Printf(output,
			currentSecond, p.execNum, clientNum, p.subscribeTotalNum, p.subscribeNum, p.publishTotalNum, p.publishNum, avgPNum, p.maxPublishNum, p.receiveTotalNum, p.receiveNum, avgRNum, p.maxReceiveNum)
		fmt.Printf("\n")
	}
}

func (p *publish) ExecPublish() {
	time.AfterFunc(time.Duration(p.cfg.Interval)*time.Second, func() {
		if p.execNum == int64(p.cfg.ExecNum) {
			alog.Info("发布已执行完成，等待接收订阅...")
			p.disconnect()
			p.execComplete <- true
			return
		}
		atomic.AddInt64(&p.execNum, 1)
		p.publish()
		p.publishInfo()
		p.ExecPublish()

	})
}

func (p *publish) disconnect() {
	if v := p.cfg.DisconnectScale; v <= 0 || v > 100 || p.cfg.AutoReconnect {
		return
	}
	clientCount := p.clients.Len()
	disCount := int(float32(clientCount) * (float32(p.cfg.DisconnectScale) / 100))
	for _, v := range p.clients.ToMap() {
		cli := v.(MQTT.MqttClienter)
		cli.Disconnect()
		//cli.Disconnect(100)
		disCount--
		if disCount == 0 {
			break
		}
	}
}

func (p *publish) publish() {
	for i, l := 0, len(p.clientData); i < l; i++ {
		clientInfo := p.clientData[i]
		pNum := len(clientInfo.Relations)
		atomic.AddInt64(&p.publishTotalNum, int64(pNum))
		atomic.AddInt64(&p.receiveTotalNum, int64(pNum*2))
		go p.userPublish(clientInfo)
	}
}

func (p *publish) userPublish(clientInfo config.ClientInfo) {
	clientID := clientInfo.ClientID
	c, _ := p.clients.Get(clientID)
	if c == nil {
		return
	}
	cli := c.(MQTT.MqttClienter)
	for j := 0; j < len(clientInfo.Relations); j++ {
		aid := uuid.Rand().Hex()
		st := time.Now().Format("20060102150405")
		sendData := fmt.Sprintf(`{
    "AID": "%s",
    "MT": "MSG",
    "ST": "%s",
    "FUID": "%s",
    "TUID": "%s",
    "CT": "wz",
    "VOICELEN": "0",
    "PICTYPE": "",
    "CONTENT": "helllo,world"
}`, aid, st, clientID, clientInfo.Relations[j])
		topic := "S/U2U"
		cli.Publish(topic, MQTT.QoS(1), false, []byte(sendData))

		// 好友之间的发包间隔
		if v := p.cfg.UserInterval; v > 0 {
			time.Sleep(time.Duration(v) * time.Millisecond)
		}
	}
}
func (p *publish) publishInfo() {
	pNum := len(p.infoClientData)
	atomic.AddInt64(&p.publishTotalNum, int64(pNum))
	atomic.AddInt64(&p.receiveTotalNum, int64(pNum))
	for _, info := range p.infoClientData {
		go p.infoPublish(info)
	}
}

func (p *publish) infoPublish(clientInfo config.ClientInfo) {
	clientID := clientInfo.ClientID
	c, _ := p.clients.Get(clientID)
	if c == nil {
		return
	}

	cli := c.(MQTT.MqttClienter)
	aid := uuid.Rand().Hex()
	st := time.Now().Format("20060102150405")
	sendData := fmt.Sprintf(`{
		    "MT": "INFOQUERYBYIDLIST",
		    "AID": "%s",
		    "ST": "%s",
		    "UID": "%s",
		    "IDLIST": [
		        "100000000144",
		        "100000000134",
		        "100000000133",
		        "100000000132",
		        "100000000131",
		        "100000000130",
		        "100000000129",
		        "100000000123",
		        "100000000119",
		        "100000000108"
		    ]
}`, aid, st, clientID)
	topic := "S/INFO"
	cli.Publish(topic, MQTT.QoS(1), false, []byte(sendData))
	// 好友之间的发包间隔
	if v := p.cfg.UserInterval; v > 0 {
		time.Sleep(time.Duration(v) * time.Millisecond)
	}

}
