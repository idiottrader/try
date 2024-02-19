package bybit_usdt_swap_v5

import (
	"fmt"
	"strings"
	"time"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

type BybitUsdtSwap struct {
	params       *helper.BrokerConfig
	pair         helper.Pair
	symbol       string
	exchangeName helper.BrokerName // 交易所名称
	pairInfo     *helper.ExchangeInfo
	client       *bybitUsdtSwapClient
	//failNum         atomic.Int64 // 出错次数
	cb              helper.CallbackFunc
	tm              *helper.TradeMsg
	colo            bool
	takerOrderPass  helper.PassTime
	cancelOrderPass helper.PassTime
	checkOrderPass  helper.PassTime
	systemPass      helper.PassTime
	//
	takerFee atomic.Float64 // taker费率
	makerFee atomic.Float64 // maker费率
}

func NewRs(params *helper.BrokerConfig, msg *helper.TradeMsg, pairInfo *helper.ExchangeInfo, cb helper.CallbackFunc) *BybitUsdtSwap {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}
	coloflag := checkColo(params)
	b := &BybitUsdtSwap{
		params:       params,
		pair:         params.Pair,
		symbol:       pairToSymbol(params.Pair),
		exchangeName: helper.BrokernameBybitUsdtSwap,
		pairInfo:     pairInfo,
		client:       NewClient(params),
		cb:           cb,
		tm:           msg,
		colo:         coloflag,
	}
	return b
}

// 获取交易规则
// todo 增加全市场exchangeinfo获取 增加文件缓存
func (b *BybitUsdtSwap) getExchangeInfo() {
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.get(
		"/v5/market/instruments-info",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
		},
		false,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息错误 需要停机. %v", b.exchangeName, value))
				return
			}
			datas := value.Get("result").GetArray("list")
			for _, data := range datas {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol == b.symbol {
					status := helper.BytesToString(data.GetStringBytes("status"))
					enabled := false
					if status == "Trading" {
						enabled = true
					}
					b.pairInfo.Pair = b.pair
					b.pairInfo.Status = enabled
					b.pairInfo.TickSize = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.Get("priceFilter").GetStringBytes("tickSize"))))
					b.pairInfo.StepSize = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("qtyStep"))))
					b.pairInfo.MinOrderAmount, _ = decimal.NewFromString(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("minOrderQty")))
					b.pairInfo.MaxOrderAmount, _ = decimal.NewFromString(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("maxOrderQty")))
					b.pairInfo.Multi = decimal.NewFromFloat(1)
					b.pairInfo.MaxOrderValue = decimal.NewFromFloat(20000)
					b.pairInfo.MinOrderValue = decimal.NewFromFloat(10)
				}
			}
		},
	)
	if err != nil {
		b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) getTicker() {
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.get(
		"/v5/market/tickers",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
		},
		false,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]get ticker failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]get ticker err. %v", b.exchangeName, value))
				return
			}
			datas := value.Get("result").GetArray("list")
			data := datas[0]
			symbol := helper.BytesToString(data.GetStringBytes("symbol"))
			if symbol == b.symbol {
				t := &b.tm.Ticker
				t.Bp.Store(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("bid1Price"))))
				t.Ap.Store(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("ask1Price"))))
				t.Mp.Store(t.Price())
				b.cb.OnTicker(time.Now().UnixMicro())
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]get ticker failed. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) getEquity() {
	p := handyPool.Get()
	defer handyPool.Put(p)
	coin := strings.ToUpper(b.pair.Quote)
	_, err := b.client.get(
		"/v5/account/wallet-balance",
		map[string]interface{}{
			"coin": coin,
		},
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]get equity failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]get equity err. %v", b.exchangeName, value))
				return
			}
			datas := value.Get("result").GetArray("list")
			data := datas[0]
			if coin == helper.BytesToString(data.GetStringBytes("coin")) {
				t := &b.tm.Equity
				t.Lock.Lock()
				t.Cash = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("usdValue")))
				t.CashFree = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("walletBalance")))
				t.Lock.Unlock()
				b.cb.OnEquity(time.Now().UnixMicro())
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]get equity failed. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) setPositionMode() {
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.post(
		"/v5/position/switch-mode",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
			"mode":     0,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]set position mode failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]set position mode err. %v", b.exchangeName, value))
				return
			}
			log.Info("set position mode successed. 单向持仓")
		},
	)
	if err != nil {
		log.Warnf("set position mode failed. %v", err.Error())
	}

}

func (b *BybitUsdtSwap) setRiskLimit() {
	// 设置风险登记，bybit一共有30个风险登记要求, 和杠杆倍数
	// 这里选的是第5档, 仓位价值100万，维持保证金3%，初始保证金6%，最大杠杆16.67
	// 仓位模式选择单向持仓
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.post(
		"/v5/position/set-risk-limit",
		map[string]interface{}{
			"category":    "linear",
			"symbol":      b.symbol,
			"riskId":      5,
			"positionIdx": 0,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]set risk limit failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]set risk limit err. %v", b.exchangeName, value))
				return
			}
			log.Info("set account successed. risk Id 5, 单向持仓")
		},
	)
	if err != nil {
		log.Warnf("set risk limit failed. %v", err.Error())
	}
}

func (b *BybitUsdtSwap) setLeverage() {
	// 杠杆率水平先设置成第五档的水平
	p := handyPool.Get()
	defer handyPool.Put(p)
	l := "15"
	_, err := b.client.post(
		"/v5/position/set-leverage",
		map[string]interface{}{
			"category":     "linear",
			"symbol":       b.symbol,
			"buyLeverage":  l,
			"sellLeverage": l,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]set leverage failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]set leverage err. %v", b.exchangeName, value))
				return
			} else {
				log.Info(fmt.Sprintf("[%s]set leverage success. %v", b.exchangeName, l))
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]get equity failed. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) cancelAllOpenOrders(only bool) { // 使用递归的方法，来批量撤销订单
	if only {
		b.cancelOpenOrders(b.symbol)
	} else {
		p := handyPool.Get()
		defer handyPool.Put(p)
		_, err := b.client.get(
			"/v5/order/realtime",
			map[string]interface{}{
				"category":   "linear",
				"settleCoin": strings.ToUpper(b.pair.Quote),
			},
			true,
			func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
				value, handlerErr := p.ParseBytes(respBody)
				if handlerErr != nil {
					log.Warn(fmt.Sprintf("[%s]cancelAllOpenOrders failed. %s", b.exchangeName, handlerErr.Error()))
					return
				}
				code := value.GetInt64("retCode")
				if code != 0 {
					log.Warn(fmt.Sprintf("[%s]cancelAllOpenOrders err. %v", b.exchangeName, value))
					return
				} else {
					symbolSet := make(map[string]struct{}, 0)
					for _, data := range value.Get("result").GetArray("list") {
						symbolSet[string(data.GetStringBytes("symbol"))] = struct{}{}
					}
					if len(symbolSet) > 0 {
						for symbol := range symbolSet {
							b.cancelOpenOrders(symbol)
							log.Infof("cancel symbol open orders. %s", symbol)
						}
						// todo 可能会出现无限循环
						//time.Sleep(3 * time.Second)
						//b.cancelAllOpenOrders(only)
					} else {
						log.Infof("cancel all symbols successed.")
					}
				}
			},
		)
		if err != nil {
			log.Warn(fmt.Sprintf("[%s]cancelAllOpenOrders failed. %s", b.exchangeName, err.Error()))
		}
	}
}

func (b *BybitUsdtSwap) cancelOpenOrders(symbol string) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.post(
		"/v5/order/cancel-all",
		map[string]interface{}{
			"category": "linear",
			"symbol":   symbol,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]cancel symbol allorders failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]cancel symbol allorders err. %v %v", b.exchangeName, symbol, value))
				return
			} else {
				log.Info(fmt.Sprintf("[%s]cancel symbol allorders success. %v", b.exchangeName, symbol))
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]cancel symbol allorders failed. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) cleanPosition(only bool) bool {
	ifleft := false // 默认遗漏仓位
	params := make(map[string]interface{}, 0)
	params["category"] = "linear"
	if only {
		params["symbol"] = b.symbol
	} else {
		params["settleCoin"] = strings.ToUpper(b.pair.Quote)
	}
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.get(
		"/v5/position/list",
		params,
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]get position failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]get position err. %v", b.exchangeName, value))
				return
			} else {
				// 手工下市价单
				for _, data := range value.Get("result").GetArray("list") {
					if !ifleft {
						ifleft = true
					}
					symbol := string(data.GetStringBytes("symbol"))
					size := string(data.GetStringBytes("size"))
					positionIdx := data.GetInt64("positionIdx")
					params := map[string]interface{}{
						"category":    "linear",
						"symbol":      symbol,
						"orderType":   "Market",
						"qty":         size,
						"positionIdx": positionIdx,
						"timeInForce": "ImmediateOrCancel",
						"reduceOnly":  true,
					}
					side := string(data.GetStringBytes("side"))
					switch side {
					case "Buy":
						params["side"] = "Sell"
					case "Sell":
						params["side"] = "Buy"
					}
					b.client.post(
						"/v5/order/create",
						params,
						func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
							log.Errorf("market close position %v", string(respBody))
						})
				}
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]get position failed. %s", b.exchangeName, err.Error()))
	}
	return ifleft
}

func (b *BybitUsdtSwap) adjustAcct() {
	b.setPositionMode()
	b.setRiskLimit()
	b.setLeverage()
}

func (b *BybitUsdtSwap) BeforeTrade(mode helper.BeforeTradeMode) {
	// 获取交易规则
	b.getExchangeInfo()
	// 清空挂单
	// 清空仓位
	switch mode {
	case helper.BeforeTradeModePrepare:
		b.getPosition()
	case helper.BeforeTradeModeCloseOne:
		b.cancelAllOpenOrders(true)
		b.cleanPosition(true)
	case helper.BeforeTradeModeCloseAll:
		b.cancelAllOpenOrders(false)
		b.cleanPosition(false)
	}
	// 调整账户
	b.adjustAcct()
	// 获取账户资金
	b.getEquity()
	// 获取ticker
	b.getTicker()
}

func (b *BybitUsdtSwap) AfterTrade(mode helper.AfterTradeMode) bool {
	var isleft bool
	switch mode {
	case helper.AfterTradeModePrepare:
	case helper.AfterTradeModeCloseOne:
		b.cancelAllOpenOrders(true)
		isleft = b.cleanPosition(true)
	case helper.AfterTradeModeCloseAll:
		b.cancelAllOpenOrders(false)
		isleft = b.cleanPosition(false)
	}
	return isleft
}

func (b *BybitUsdtSwap) getPosition() {
	params := make(map[string]interface{}, 0)
	params["symbol"] = b.symbol
	params["category"] = "linear"
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := b.client.get(
		"/v5/position/list",
		params,
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]get position failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]get position err. %v", b.exchangeName, value))
				return
			}
			datas := value.Get("result").GetArray("list")
			if len(datas) > 0 {
				for _, data := range datas {
					symbol := helper.BytesToString(data.GetStringBytes("symbol"))
					if symbol != b.symbol {
						continue
					}
					size, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("size")))
					price, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("avgPrice")))
					side := helper.BytesToString(data.GetStringBytes("side"))
					b.tm.Position.Lock.Lock()
					switch side {
					case "Buy":
						b.tm.Position.LongPos = size
						b.tm.Position.LongAvg = price.InexactFloat64()
					case "Sell":
						b.tm.Position.ShortPos = size
						b.tm.Position.ShortAvg = price.InexactFloat64()
					default:
						b.tm.Position.LongPos = decimal.Zero
						b.tm.Position.LongAvg = 0
						b.tm.Position.ShortPos = decimal.Zero
						b.tm.Position.ShortAvg = 0
					}
					b.tm.Position.Lock.Unlock()
					b.cb.OnPosition(time.Now().UnixMicro())
				}
			} else {
				b.tm.Position.Reset()
				b.cb.OnPosition(time.Now().UnixMicro())
			}
		},
	)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s]get position failed. %s", b.exchangeName, err.Error()))
	}
}

func (b *BybitUsdtSwap) takeOrder(price float64, size decimal.Decimal, cid string, side helper.OrderSide, orderType helper.OrderType, t int64) {
	params := map[string]interface{}{
		"category":    "linear",
		"symbol":      b.symbol,
		"qty":         size.String(),
		"orderLinkId": cid,
	}
	switch side {
	case helper.OrderSideKD, helper.OrderSidePK:
		params["side"] = "Buy"
	case helper.OrderSidePD, helper.OrderSideKK:
		params["side"] = "Sell"
	default:
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeERROR
		order.ClientID = cid
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		log.Errorf("[%s]%s下单失败 错误的订单方向 %v", b.exchangeName, cid, side)
		return
	}
	switch orderType {
	case helper.OrderTypeLimit:
		params["orderType"] = "Limit"
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
	case helper.OrderTypeIoc:
		params["orderType"] = "Limit"
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
		params["timeInForce"] = "IOC"
	case helper.OrderTypePostOnly:
		params["orderType"] = "Limit"
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
		params["timeInForce"] = "PostOnly"
	case helper.OrderTypeMarket:
		params["orderType"] = "Market"
	default:
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeERROR
		order.ClientID = cid
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		log.Errorf("[%s]%s下单失败 错误的订单类型 %v", b.exchangeName, cid, orderType)
		return
	}
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0

	b.systemPass.Update(time.Now().UnixMicro() - t)

	code, err := b.client.post(
		"/v5/order/create",
		params,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]take order failed. %s", b.exchangeName, handlerErr.Error()))
				var order helper.OrderEvent
				order.Type = helper.OrderEventTypeERROR
				order.ClientID = cid
				b.cb.OnOrder(time.Now().UnixMicro(), order)
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]take order err. %v", b.exchangeName, value))
				var order helper.OrderEvent
				order.Type = helper.OrderEventTypeERROR
				order.ClientID = cid
				b.cb.OnOrder(time.Now().UnixMicro(), order)
				return
			}

			oid := string(value.Get("result").GetStringBytes("orderId"))
			cid := string(value.Get("result").GetStringBytes("orderLinkId"))
			var order helper.OrderEvent
			order.Type = helper.OrderEventTypeNEW
			order.OrderID = oid
			order.ClientID = cid
			b.cb.OnOrder(time.Now().UnixMicro(), order)
		},
	)
	if code == 403 {
		b.cb.OnExit("触发限频 紧急停机")
	}
	if err != nil {
		//得检查是否有限频提示
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeERROR
		order.ClientID = cid
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		log.Errorf("[%s]%s下单失败 %s", b.exchangeName, cid, err.Error())
	}
	if pass > 0 {
		b.takerOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) amendOrder(oid string, price float64, size decimal.Decimal) {
	// 注意：改单失败会去撤单
	params := map[string]interface{}{
		"category": "linear",
		"symbol":   b.symbol,
		"orderId":  oid,
		"price":    helper.FixPrice(price, b.pairInfo.TickSize).String(),
	}
	if size.GreaterThanOrEqual(b.pairInfo.MinOrderAmount) {
		params["qty"] = size.String()
	}
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0
	code, err := b.client.post(
		"/v5/order/amend",
		params,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]amend order failed. %s", b.exchangeName, handlerErr.Error()))
				b.cancelOrder(oid, 0)
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]amend order err. %v", b.exchangeName, value))
				b.cancelOrder(oid, 0)
				return
			}
			// 改单成功没有操作
		},
	)
	if code == 403 {
		b.cb.OnExit("触发限频 紧急停机")
	}
	if err != nil {
		//得检查是否有限频提示
		log.Errorf("[%s]%s改单失败 %s", b.exchangeName, oid, err.Error())
		b.cancelOrder(oid, 0)
	}
	if pass > 0 {
		b.takerOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) cancelOrder(oid string, t int64) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0

	if t > 0 {
		b.systemPass.Update(time.Now().UnixMicro() - t)
	}

	code, err := b.client.post(
		"/v5/order/cancel",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
			"orderId":  oid,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]cancel order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]cancel order err. %v", b.exchangeName, value))
				return
			}
		},
	)
	if code == 403 {
		b.cb.OnExit("触发限频 紧急停机")
	}
	if err != nil {
		log.Warnf("[%s] 撤单失败 %v", b.exchangeName, err.Error())
	}
	if pass > 0 {
		b.cancelOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) cancelOrderCid(cid string, t int64) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0

	if t > 0 {
		b.systemPass.Update(time.Now().UnixMicro() - t)
	}

	code, err := b.client.post(
		"/v5/order/cancel",
		map[string]interface{}{
			"category":    "linear",
			"symbol":      b.symbol,
			"orderLinkId": cid,
		},
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Warn(fmt.Sprintf("[%s]cancel order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Warn(fmt.Sprintf("[%s]cancel order err. %v", b.exchangeName, value))
				return
			}
		},
	)
	if code == 403 {
		b.cb.OnExit("触发限频 紧急停机")
	}
	if err != nil {
		log.Warnf("[%s] 撤单失败 %v", b.exchangeName, err.Error())
	}
	if pass > 0 {
		b.cancelOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) checkOrderCid(cid string) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0
	_, err := b.client.get(
		"/v5/order/realtime",
		map[string]interface{}{
			"category":    "linear",
			"symbol":      b.symbol,
			"orderLinkId": cid,
		},
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Info(fmt.Sprintf("[%s]check order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Info(fmt.Sprintf("[%s]check order err. %v", b.exchangeName, value))
				return
			}
			finded := false
			for _, data := range value.Get("result").GetArray("list") {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol != b.symbol {
					continue
				}
				var order helper.OrderEvent
				order.OrderID = string(data.GetStringBytes("orderId"))
				order.ClientID = string(data.GetStringBytes("orderLinkId"))
				status := helper.BytesToString(data.GetStringBytes("orderStatus"))
				switch status {
				case "Filled", "Cancelled":
					order.Type = helper.OrderEventTypeREMOVE
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					if !order.Filled.IsZero() {
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("avgPrice")))
						order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
					}
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				finded = true
				b.cb.OnOrder(time.Now().UnixMicro(), order)
			}
			if !finded {
				go b.checkOrderCidHistory(cid)
			}
		},
	)
	if err != nil {
		log.Infof("[%s] 查单失败 %v", b.exchangeName, err.Error())
	}
	if pass > 0 {
		b.checkOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) checkOrder(oid string) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	start := time.Now().UnixMilli()
	var pass int64 = 0
	_, err := b.client.get(
		"/v5/order/realtime",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
			"orderId":  oid,
		},
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			pass = time.Now().UnixMilli() - start
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Info(fmt.Sprintf("[%s]check order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Info(fmt.Sprintf("[%s]check order err. %v", b.exchangeName, value))
				return
			}
			finded := false
			for _, data := range value.Get("result").GetArray("list") {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol != b.symbol {
					continue
				}
				var order helper.OrderEvent
				order.OrderID = string(data.GetStringBytes("orderId"))
				order.ClientID = string(data.GetStringBytes("orderLinkId"))
				status := helper.BytesToString(data.GetStringBytes("orderStatus"))
				switch status {
				case "Filled", "Cancelled":
					order.Type = helper.OrderEventTypeREMOVE
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					if !order.Filled.IsZero() {
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("avgPrice")))
						order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
					}
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				finded = true
				b.cb.OnOrder(time.Now().UnixMicro(), order)
			}
			if !finded {
				b.checkOrderHistory(oid)
			}
		},
	)
	if err != nil {
		log.Infof("[%s] 查单失败 %v", b.exchangeName, err.Error())
	}
	if pass > 0 {
		b.checkOrderPass.Update(pass)
	}
}

func (b *BybitUsdtSwap) checkOrderHistory(oid string) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	b.client.get(
		"/v5/order/history",
		map[string]interface{}{
			"category": "linear",
			"symbol":   b.symbol,
			"orderId":  oid,
		},
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Info(fmt.Sprintf("[%s]check history order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Info(fmt.Sprintf("[%s]check history order err. %v", b.exchangeName, value))
				return
			}
			for _, data := range value.Get("result").GetArray("list") {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol != b.symbol {
					continue
				}
				var order helper.OrderEvent
				order.OrderID = string(data.GetStringBytes("orderId"))
				order.ClientID = string(data.GetStringBytes("orderLinkId"))
				status := helper.BytesToString(data.GetStringBytes("orderStatus"))
				switch status {
				case "Filled", "Cancelled":
					order.Type = helper.OrderEventTypeREMOVE
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					if !order.Filled.IsZero() {
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("avgPrice")))
						order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
					}
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				b.cb.OnOrder(time.Now().UnixMicro(), order)
			}
		},
	)
}

func (b *BybitUsdtSwap) checkOrderCidHistory(cid string) {
	p := handyPool.Get()
	defer handyPool.Put(p)
	b.client.get(
		"/v5/order/history",
		map[string]interface{}{
			"category":    "linear",
			"symbol":      b.symbol,
			"orderLinkId": cid,
		},
		true,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				log.Info(fmt.Sprintf("[%s]check history order failed. %s", b.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				log.Info(fmt.Sprintf("[%s]check history order err. %v", b.exchangeName, value))
				return
			}
			for _, data := range value.Get("result").GetArray("list") {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol != b.symbol {
					continue
				}
				var order helper.OrderEvent
				order.OrderID = string(data.GetStringBytes("orderId"))
				order.ClientID = string(data.GetStringBytes("orderLinkId"))
				status := helper.BytesToString(data.GetStringBytes("orderStatus"))
				switch status {
				case "Filled", "Cancelled":
					order.Type = helper.OrderEventTypeREMOVE
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					if !order.Filled.IsZero() {
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("avgPrice")))
						order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
					}
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				b.cb.OnOrder(time.Now().UnixMicro(), order)
			}
		},
	)
}

func (b *BybitUsdtSwap) GetDelay() (int64, int64, int64) {
	return b.takerOrderPass.GetDelay(), b.cancelOrderPass.GetDelay(), b.systemPass.GetDelay()
}

func (b *BybitUsdtSwap) GetFee() (float64, float64) {
	return b.takerFee.Load(), b.makerFee.Load()
}

// func (b *BybitUsdtSwap) GetAcctSum() helper.AcctSum {
// 	var a helper.AcctSum
// 	return a
// }

func (b *BybitUsdtSwap) SendSignal(signals []helper.Signal) {
	for _, s := range signals {
		log.Debugf("发送信号 %s", s)
		switch s.Type {
		case helper.SignalTypeNewOrder:
			go b.takeOrder(s.Price, s.Amount, s.ClientID, s.OrderSide, s.OrderType, s.Time)
		case helper.SignalTypeCancelOrder:
			if s.OrderID != "" {
				go b.cancelOrder(s.OrderID, s.Time)
			} else {
				go b.cancelOrderCid(s.ClientID, s.Time)
			}
		case helper.SignalTypeAmend:
			if s.OrderID != "" {
				go b.amendOrder(s.OrderID, s.Price, s.Amount)
			}
		case helper.SignalTypeCheckOrder:
			if s.OrderID != "" {
				go b.checkOrder(s.OrderID)
			} else {
				go b.checkOrderCid(s.ClientID)
			}
		case helper.SignalTypeGetPos:
			go b.getPosition()
		case helper.SignalTypeGetEquity:
			go b.getEquity()
		}
	}
}
