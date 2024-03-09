package acxfinance_usdt_swap

import (
	"fmt"
	"strconv"
	"testing"
	"time"
	"zbeast/pkg/quant/broker/testconfig"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"
)

var (
	localOrder = make(map[string]helper.OrderEvent)
	msg        = &helper.TradeMsg{}
	info       = &helper.ExchangeInfo{}
	pair, _    = helper.StringPairToPair("ltc_usdc")
)

func makeRs() *AcxfinanceUsdtSwap {
	log.Init("", log.DebugLevel,
		//log.Init("", log.InfoLevel,
		log.SetStdout(true),
		//log.SetSLog(true),
		log.SetCaller(true),
	)
	c := testconfig.LoadTestConfig()
	return NewRs(
		&helper.BrokerConfig{
			AccessKey: c.AccessKey,
			SecretKey: c.SecretKey,
			PassKey:   c.PassKey,
			ProxyURL:  c.ProxyURL,
			Pair:      pair,
		},
		msg,
		info,
		helper.CallbackFunc{
			OnTicker: func(ts int64) {
				fmt.Println("ticker:", msg.Ticker)
			},
			OnEquity: func(ts int64) {
				fmt.Println("equity:", msg.Equity.String())
			},
			OnOrder: func(ts int64, event helper.OrderEvent) {
				fmt.Println("order:", event)
				localOrder[event.ClientID] = event
			},
			OnDepth: func(ts int64) {
			},
			OnPosition: func(ts int64) {
				fmt.Println("position:", msg.Position.String())
			},
			OnExit: func(msg string) {
				fmt.Println("exit:", msg)
			},
			OnReset: func(msg string) {
				fmt.Println("rest:", msg)
			},
		},
	)
}

func TestRs(t *testing.T) {
	w := makeRs()
	w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	time.Sleep(10 * time.Second)
	t.Logf("after trade %v", w.AfterTrade(helper.AfterTradeModeCloseOne))
}

func TestExistPosition(t *testing.T) {
	//SetSim()
	//w := makeRs()
	//only := true
	//t.Logf("after trade %v", w.cleanPosition(only))
}

func TestGetPosition(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	//time.Sleep(10 * time.Second)
	w.getPosition()
	//time.Sleep(10 * time.Second)
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
}

func TestGetEquity(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	//time.Sleep(10 * time.Second)
	//w.getExchangeInfo()
	w.getFees()
	//w.getEquity()
	//time.Sleep(10 * time.Second)
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
}

func TestSetLeverRate(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	//time.Sleep(10 * time.Second)
	w.setLeverRate()
	//time.Sleep(10 * time.Second)
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
}

func TestGetTicker(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	//time.Sleep(10 * time.Second)
	w.getTicker()
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
}

func TestCancelOpenOrders(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	//time.Sleep(10 * time.Second)
	w.cancelAllOpenOrders(true)
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
}

func TestSendSignal(t *testing.T) {
	//SetSim()
	w := makeRs()
	w.BeforeTrade(helper.AfterTradeModeCloseOne)
	w.getTicker()
	time.Sleep(10 * time.Second)

	// 获取tick
	time.Sleep(time.Second)
	cid := strconv.Itoa(int(time.Now().UnixMilli()))
	targetPrice := msg.Ticker.Mp.Load() * 1.01
	notional := 10.0
	lever := 5.0
	size := notional * lever / targetPrice
	amount := decimal.NewFromFloat(size)
	// 超价 买一单
	w.SendSignal([]helper.Signal{{
		Type:      helper.SignalTypeNewOrder,
		ClientID:  cid,
		OrderID:   "",
		Price:     targetPrice,
		Amount:    amount,
		OrderSide: helper.OrderSideKD,
		OrderType: helper.OrderTypeMarket,
	}})

	// 等订单推送
	cnt := 0
	for {
		time.Sleep(time.Second)
		_, ok := localOrder[cid]
		if ok {
			break
		}
		cnt++
		if cnt > 10 {
			t.Fatal("no order id")
		}
	}

	// 查单
	order := localOrder[cid]
	w.SendSignal([]helper.Signal{{
		Type:     helper.SignalTypeCheckOrder,
		ClientID: cid,
		OrderID:  order.OrderID,
	}})

	time.Sleep(time.Second)

	if !order.Filled.LessThanOrEqual(amount) {
		t.Errorf("订单大小不符合预期:%s : %s", order.Filled.String(), amount.String())
	}

	// 查仓位
	w.getPosition()
	time.Sleep(time.Second)
	if !msg.Position.LongPos.LessThanOrEqual(amount) {
		t.Errorf("仓位大小不符合预期:%s : %s", msg.Position.LongPos.String(), amount.String())
	}
	// 清场
	for i := 0; i < 5; i++ {
		ret := w.AfterTrade(helper.AfterTradeModeCloseOne)
		if !ret {
			break
		}
		time.Sleep(time.Second)
	}
}

func TestCancelOrderId(t *testing.T) {
	//SetSim()
	w := makeRs()
	clientOrderId := "391234013"
	orderId := "000001005335786"
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	w.cancelOrderID(orderId, clientOrderId)
}

func TestOrderId(t *testing.T) {
	//SetSim()
	w := makeRs()
	clientOrderId := "391234013"
	orderId := "000001005335786"
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	w.checkOrderID(orderId, clientOrderId)
}

//resp:[200] {"code":0,"msg":"","data":{"symbol":"LTC-USDC","side":"BUY","positionSide":"NET","status":"CANCELED","price":"62.00","origQty":"1.000","origType":"","type":"LIMIT","timeInForce":"GTC","orderId":"000001005335786","clientOrderId":"391234013","reduceOnly":"false","workingType":"LAST_PRICE","stopPrice":"0.00","closePosition":"","activationPrice":"0.00","priceRate":"0.00","priceProtect":"","orderTime":1684259563767,"cumQuote":"0.00","executedQty":"0.000","avgPrice":"0.00","updateTime":1684259679073}}
//resp:[200] {"code":0,"msg":"","data":{"symbol":"LTC-USDC","side":"BUY","positionSide":"NET","status":"NEW","price":"62.00","origQty":"1.000","origType":"","type":"LIMIT","timeInForce":"GTC","orderId":"000001005335786","clientOrderId":"391234013","reduceOnly":"false","workingType":"LAST_PRICE","stopPrice":"0.00","closePosition":"","activationPrice":"0.00","priceRate":"0.00","priceProtect":"","orderTime":1684259563767,"cumQuote":"0.00","executedQty":"0.000","avgPrice":"0.00","updateTime":0}}

//{"code":0,"msg":"","data":{"clientOrderId":"391234013","orderId":"000001005335786"}}

// 测试 before/after trade ws 下单的影响
func TestWsNewOrder(t *testing.T) {
	//SetSim()
	w := makeRs()
	//w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	w.getExchangeInfo()
	w.getFees()
	//time.Sleep(time.Second)
	//w.AfterTrade(helper.AfterTradeModeCloseOne)
	//time.Sleep(time.Second)
	//w.BeforeTrade(helper.AfterTradeModeCloseOne)
	//time.Sleep(time.Second)
	//log.Debugf("发送订单")
	w.SendSignal([]helper.Signal{
		{
			Time: time.Now().UnixMilli(),
			Type: helper.SignalTypeNewOrder,
			//Type:     helper.OrderTypePostOnly,
			ClientID: "652234113",
			//OrderID:   "5561234564",
			Price:     99.0,
			Amount:    decimal.NewFromFloat(1),
			OrderSide: helper.OrderSideKD,
			OrderType: helper.OrderTypeLimit,
			//OrderType: helper.OrderTypePostOnly,
			//timeInForce
			//priceProtect
		},
		//orderParams := map[string]interface{}{
		//      "symbol":        "ETH-USDC",
		//      "side":          "BUY",
		//      "type":          "LIMIT",
		//      "quantity":      0.001,
		//      "price":         1715,
		//      "clientOrderId": "250000013",
		//      "timeInForce":   "GTC",
		//}
	})
	//time.Sleep(time.Second * 5)
}

//{"code":0,"msg":"","data":{"clientOrderId":"391230013","orderId":"000001005294905"}
