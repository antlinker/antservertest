package publish

import (
	"sync/atomic"

	"github.com/antlinker/alog"
	"github.com/antlinker/go-mqtt/packet"

	MQTT "github.com/antlinker/go-mqtt/client"
)

func newHandleConnect(clientID string, pub *publish) *handleConnect {
	return &handleConnect{
		clientID: clientID,
		pub:      pub,
	}
}

type handleConnect struct {
	MQTT.DefaultConnListen
	MQTT.DefaultSubscribeListen
	MQTT.DefaultPubListen
	MQTT.DefaultDisConnListen
	clientID      string
	pub           *publish
	recvPacketCnt int64
	sendPacketCnt int64
}

func (hc *handleConnect) OnConnStart(event *MQTT.MqttConnEvent) {

}
func (hc *handleConnect) OnConnSuccess(event *MQTT.MqttConnEvent) {

}
func (hc *handleConnect) OnConnFailure(event *MQTT.MqttConnEvent, returncode int, err error) {
	alog.Debugf("OnConnFailure(%d):%v", returncode, err)
	if !hc.pub.cfg.AutoReconnect {
		hc.pub.clients.Remove(hc.clientID)
	}
}
func (hc *handleConnect) OnRecvPublish(event *MQTT.MqttRecvPubEvent, topic string, message []byte, qos MQTT.QoS) {
	atomic.AddInt64(&hc.pub.receiveNum, 1)

}
func (hc *handleConnect) OnUnSubStart(event *MQTT.MqttEvent, filter []string) {
	alog.Debugf("OnUnSubscribeStart:%v", filter)
}
func (*handleConnect) OnSubStart(event *MQTT.MqttEvent, sub []MQTT.SubFilter) {
	alog.Debugf("OnSubscribeStart:%v", sub)
}
func (hc *handleConnect) OnSubSuccess(event *MQTT.MqttEvent, sub []MQTT.SubFilter, result []MQTT.QoS) {

	atomic.AddInt64(&hc.pub.subscribeNum, 1)
	hc.pub.clients.Set(hc.clientID, event.GetClient())
}
func (hc *handleConnect) OnRecvPacket(event *MQTT.MqttEvent, packet packet.MessagePacket, recvPacketCnt int64) {
	rc := atomic.AddInt64(&hcrecvPacketCnt, 1)
	alog.Debugf("OnRecvPacket:%d", rc)
}
func (hc *handleConnect) OnSendPacket(event *MQTT.MqttEvent, packet packet.MessagePacket, sendPacketCnt int64, err error) {
	sc := atomic.AddInt64(&hcsendPacketCnt, 1)
	alog.Debugf("OnSendPacket:%d", sc)
}

func (hc *handleConnect) OnPubSuccess(event *MQTT.MqttPubEvent, mp *MQTT.MqttPacket) {
	atomic.AddInt64(&hc.pub.publishNum, 1)
}

var hcrecvPacketCnt int64
var hcsendPacketCnt int64
var recvSubCnt int64
