package publish

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/antlinker/alog"

	"zhangshangxiaoyuan.link/antserver/restcli"
)

// Config 配置
type Config struct {
	ExecNum         int // 执行次数
	Interval        int // 发布间隔
	UserInterval    int // 好友发包间隔（单位毫秒）
	DisconnectScale int // 发送完成之后，断开客户端的比例
	Network         string
	Address         string
	Auth            string
	AutoReconnect   bool // 自动重连
	CleanSession    bool
	KeepAlive       int
	ClientCnt       int
	StartCode       int
}

// _HTTPJSONResult 请求远程数据协议http 发送json数据，返回json数据
func postr(urlStr string, payload interface{}, result interface{}) restcli.ErrHTTPJson {
	var method = "Post"
	alog.Debugf("%s请求:%s", method, urlStr)
	var s string
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return restcli.ErrReqCodeSendData
		}
		s = fmt.Sprintf("%s", b)
	}
	r, err := http.NewRequest(method, urlStr, strings.NewReader(s))
	if err != nil {
		return restcli.ErrReqCodeReqeustFaild.Cre(err)
	}
	r.Header.Set("Accept-Encoding", "gzip")
	if payload != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return restcli.ErrReqCodeRemoteFaild.Cre(err)
	}
	if resp.StatusCode != 200 {
		alog.Warnf("请求错误:%s(%d)", resp.Status, resp.StatusCode)
		if s != "" {
			alog.Warnf("发送数据：%s", s)
		}
		return restcli.ErrRespCodeFail
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return restcli.ErrRespCodeRead
	}
	alog.Debugf("返回数据:%s", string(data))

	err = json.Unmarshal(data, result)
	if err != nil {
		alog.Warnf("请求%s==>%s,返回数据:<%s>;格式错误,错误原因:%v", method, urlStr, data, err)
		return restcli.ErrRespCodeDataFormat.Cre(err)
	}

	return errHTTPJson{}
}

// errHTTPJson 错误
type errHTTPJson struct {
	code int
	msg  string
}

func (e errHTTPJson) Code() int {
	return e.code
}
func (e errHTTPJson) Error() string {
	return e.msg
}
func (e errHTTPJson) Cre(err error) errHTTPJson {
	e.msg = err.Error()
	return e
}
