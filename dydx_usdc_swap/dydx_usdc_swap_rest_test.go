package dydx_usdc_swap

import (
	"fmt"
	"strconv"
	"testing"
	"time"
	"zbeast/pkg/quant/broker/testconfig"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"

	//"zbeast/third/decimal"
	"zbeast/third/log"
)

var (
	localOrder = make(map[string]helper.OrderEvent)
	msg        = &helper.TradeMsg{}
	info       = &helper.ExchangeInfo{}
	pair, _    = helper.StringPairToPair("eth_usdc")
)

func makeRs() *DydxUsdcSwap {
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
			//WalletAddress:   c.WalletAddress,
			//StarkPrivateKey: c.StarkPrivateKey,
			ProxyURL: c.ProxyURL,
			Pair:     pair,
		},
		msg,
		info,
		helper.CallbackFunc{
			OnTicker: func(ts int64) {
				fmt.Println("ticker:", &msg.Ticker)
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

func TestPairToSymbol(t *testing.T) {
	fmt.Println(pairToSymbol(pair))
}

func TestGetExchangeInfo(t *testing.T) {
	//SetSim()
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.getExchangeInfo()
}

func TestGetTicker(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.getTicker()
}

func TestGetEquity(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.getEquity()
}

func TestGetFees(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.getFees()
}

func TestGetAllPositions(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.cleanAllPositions(true)
}

func TestGetOnePosition(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.cleanOnePosition(true)
}

func TestCancelAllOpenOrders(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.cancelAllOpenOrders(true)
}

func TestCancelOneOpenOrder(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.cancelOneOpenOrder(true)
}

func TestGetAllOrders(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.getActiveOrders(true)
	//w.getAllFillsOrders()
	//w.getFillsOrders("056594f38a26aefb6be9a86d3b79292bcdf4baa5a03d26334ec23e96de058f8")
}

func TestCheckOrderByID(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	//w.checkOrderByID("", "5f53877419a7c1ed4a6d62a81e42ff9e9445b0908e42ac189ccf9ddb75b1874")
	//w.checkOrderByID("", "0e1cf457ce726ebb661e0ec0dbb8090ccabca3782dbc4c8b16f879ca5561b1c")
	w.checkOrderByID("a663638223", "")
}

func TestCancelOrderByID(t *testing.T) {
	w := makeRs()
	time.Sleep(2 * time.Second)
	w.cancelOrderByID("425f075e604443e60a84b6b065c1f8ff11125aa8704f1bd0fc88f6543146a74")
	//w.cancelOrderByID("9803411215321134", "")
}

func TestTakeOrder(t *testing.T) {
	w := makeRs()
	//w.getExchangeInfo()
	//w.getFees()
	w.takeOrder(89.0, decimal.NewFromFloat(1), "1234567", helper.OrderSideKD, helper.OrderTypeLimit, time.Now().UnixMilli())
}

func TestWsNewOrder(t *testing.T) {
	//SetSim()
	w := makeRs()
	w.BeforeTrade(helper.BeforeTradeModeCloseOne)
	w.getExchangeInfo()
	w.getFees()
	time.Sleep(time.Second)
	w.AfterTrade(helper.AfterTradeModeCloseOne)
	time.Sleep(time.Second)
	w.BeforeTrade(helper.AfterTradeModeCloseOne)
	time.Sleep(time.Second)
	log.Debugf("发送订单")
	w.SendSignal([]helper.Signal{
		{
			Time: time.Now().UnixMilli(),
			Type: helper.SignalTypeNewOrder,
			//Type:     helper.OrderTypePostOnly,
			ClientID: strconv.Itoa(int(time.Now().Unix())),
			//OrderID:   "5561234564",
			Price:     1763.000000,
			Amount:    decimal.NewFromFloat(0.1),
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
	time.Sleep(time.Second * 5)
}
