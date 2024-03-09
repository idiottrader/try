package dydx_usdc_swap

import (
	"fmt"
	"testing"
	"time"
	"zbeast/pkg/quant/broker/testconfig"
	"zbeast/pkg/quant/helper"

	jsoniter "github.com/json-iterator/go"
)

func TestJsonMashal(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	p := map[string]interface{}{
		"op": "subscribe",
		"args": []interface{}{
			map[string]interface{}{
				"instId":  "BTC-USDT-SWAP",
				"channel": "bbo-tbt",
			},
		},
	}
	msg, err := json.Marshal(p)
	if err != nil {
		t.Errorf("[ws] json encode error , %s", err)
	}
	t.Logf("%s", string(msg))
}

func Test(t *testing.T) {
	//SetSim()
	c := testconfig.LoadTestConfig()
	msg := helper.TradeMsg{}
	pair, _ := helper.StringPairToPair("eth_usd")
	w := NewWs(
		&helper.BrokerConfig{
			Name:       "dydx_usdc_swap_ws",
			AccessKey:  c.AccessKey,
			SecretKey:  c.SecretKey,
			PassKey:    c.PassKey,
			Pair:       pair,
			NeedAuth:   false,
			NeedTicker: true,
			NeedDepth:  false,
			NeedTrade:  false,
			ProxyURL:   c.ProxyURL,
		},
		&msg,
		&helper.ExchangeInfo{},
		helper.CallbackFunc{
			OnTicker: func(ts int64) {
				t.Log(time.Now().UnixMicro(), " ticker: ", fmt.Sprintf("%v", msg.Ticker))
			},
			OnDepth: func(ts int64) {
				t.Log("depth:", msg.Depth.Price())
			},
			OnTrade: func(ts int64) {
				t.Log(msg.Trade.Get())
			},
			OnOrder: func(ts int64, o helper.OrderEvent) {
				t.Log("order:", o)
			},
			OnPosition: func(ts int64) {
			},
			OnEquity: func(ts int64) {
				t.Log(msg.Equity.String())
			},
			OnExit: func(msg string) {
				t.Log("exit:", msg)
			},
		})
	w.Run()
	<-time.After(time.Minute * 15)
	w.Stop()
}
