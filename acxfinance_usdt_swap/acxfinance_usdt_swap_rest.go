package acxfinance_usdt_swap

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"zbeast/pkg/quant/broker/client/rest"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

const (
	BaseUrl = "https://app-api.acx.finance/arbitrum"
)

var (
	handyPool fastjson.ParserPool
)

type AcxfinanceUsdtSwap struct {
	params       *helper.BrokerConfig           // 配置
	pair         helper.Pair                    // 交易对 内部
	symbol       string                         // 交易所 外部
	client       *rest.Client                   // 通用rest客户端
	cb           helper.CallbackFunc            // 所有的回调函数
	tradeMsg     *helper.TradeMsg               // 交易常用数据结构
	exchangeName helper.BrokerName              // 交易所名称
	exchangeInfo map[string]helper.ExchangeInfo // 交易信息集合
	failNum      atomic.Int64                   // 出错次数
	// 适用于pair的交易规则 避免每次使用都要查询
	pairInfo *helper.ExchangeInfo
	// 延迟统计相关变量
	takerOrderPass  helper.PassTime
	cancelOrderPass helper.PassTime
	//checkOrderPass  helper.PassTime
	systemPass helper.PassTime
	takerFee   atomic.Float64 // taker费率
	makerFee   atomic.Float64 // maker费率
}

// 创建新的实例
func NewRs(params *helper.BrokerConfig, msg *helper.TradeMsg, pairInfo *helper.ExchangeInfo, cb helper.CallbackFunc) *AcxfinanceUsdtSwap {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}
	b := &AcxfinanceUsdtSwap{
		client:       rest.NewClient(params.ProxyURL, params.LocalAddr),
		params:       params,
		exchangeName: helper.BrokernameAcxfinanceUsdtSwap,
		pair:         params.Pair,
		cb:           cb,
		pairInfo:     pairInfo,
		exchangeInfo: map[string]helper.ExchangeInfo{},
	}
	b.symbol = pairToSymbol(b.pair)
	b.tradeMsg = msg
	return b
}

// pairToSymbol 内部交易对pair转换为交易所规范的symbol btc_usdt to BTCUSDT
func pairToSymbol(pair helper.Pair) string {
	return strings.ToUpper(pair.Base + "-" + pair.Quote)
}

// 获取全市场交易规则
func (b *AcxfinanceUsdtSwap) getExchangeInfo() []*helper.ExchangeInfo {
	// 请求必备信息
	url := "/api/v1/public/instruments"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var value *fastjson.Value
	// 待使用数据结构
	infos := make([]*helper.ExchangeInfo, 0)
	// 发起请求
	err := b.call(http.MethodGet, url, nil, false, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		var handlerErr error
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received respBody: %s\n", respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", b.exchangeName, handlerErr.Error()))
			return
		}
		datas := value.GetArray("data")
		for _, data := range datas {
			symbol := string(data.GetStringBytes("symbol"))
			enableTrade := string(data.GetStringBytes("status")) == "Listed"
			minSize, err := decimal.NewFromString(strconv.FormatFloat(data.GetFloat64("minOrderSize"), 'f', -1, 64))
			if err != nil {
				log.Errorf("acxfinance minSize %v", err)
				return
			}
			info := helper.ExchangeInfo{
				Pair:           helper.Pair{Base: string(data.GetStringBytes("symbol")), Quote: string(data.GetStringBytes("asset"))},
				Symbol:         symbol,
				Status:         enableTrade,
				TickSize:       decimal.NewFromFloat(data.GetFloat64("priceTick")),
				StepSize:       decimal.NewFromFloat(data.GetFloat64("quantityStep")),
				MaxOrderAmount: decimal.NewFromFloat(1000000000),
				MinOrderAmount: minSize,
				MaxOrderValue:  decimal.NewFromFloat(30000),
				MinOrderValue:  decimal.NewFromFloat(10),
				Multi:          decimal.NewFromFloat(helper.ConvIntToFixed(data.GetInt64("multiplier"))),
			}
			infos = append(infos, &info)
			// 适用于pair的交易规则要直接保存一份
			if symbol == b.symbol {
				b.pairInfo.Pair = b.pair
				b.pairInfo.TickSize = info.TickSize
				b.pairInfo.StepSize = info.StepSize
				b.pairInfo.MaxOrderValue = info.MaxOrderValue
				b.pairInfo.MinOrderValue = info.MinOrderValue
				b.pairInfo.MaxOrderAmount = info.MaxOrderAmount
				b.pairInfo.MinOrderAmount = info.MinOrderAmount
				b.pairInfo.Multi = info.Multi
				b.pairInfo.Status = info.Status
			}
		}
	})
	if err != nil {
		b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", b.exchangeName, err.Error()))
		return nil
	}
	return infos
}

// getTicker 获取ticker行情 函数本身不需要返回值 通过callbackFunc传递出去
func (b *AcxfinanceUsdtSwap) getTicker() {
	// 请求必备信息
	url := "/api/v1/market/depth"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	params["limit"] = 1
	// 请求必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var err error
	var value, innerData *fastjson.Value
	var handlerErr error
	err = b.call(http.MethodGet, url, params, false, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		if handlerErr != nil {
			return
		}
		innerDataStr := string(value.GetStringBytes("data"))
		innerData, handlerErr = p.Parse(innerDataStr)
		if handlerErr != nil {
			return
		}
		asks := innerData.GetArray("asks")
		bids := innerData.GetArray("bids")
		if len(asks) == 0 || len(bids) == 0 {
			return
		}
		ask := asks[0]
		ap := ask.GetFloat64("0")
		bid := bids[0]
		bp := bid.GetFloat64("0")
		b.tradeMsg.Ticker.Ap.Store(ap)
		b.tradeMsg.Ticker.Bp.Store(bp)
		b.tradeMsg.Ticker.Mp.Store(b.tradeMsg.Ticker.Price())
		b.cb.OnTicker(time.Now().UnixMicro())
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warnf("get ticker err %v", err)
	}
}

// getEquity 获取账户资金 函数本身无返回值 仅更新本地资产并通过callbackfunc传递出去
func (b *AcxfinanceUsdtSwap) getEquity() {
	url := "/api/v1/account/balance"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		if handlerErr != nil {
			return
		}
		data := value.Get("data")
		available := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("totalAvailableBalance")))
		frozen := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("totalFrozenMargin")))
		margin := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("totalPositionMargin")))
		unreal := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("totalUnrealizedProfit")))
		b.tradeMsg.Equity.Lock.Lock()
		b.tradeMsg.Equity.Cash = available + frozen + margin + unreal
		b.tradeMsg.Equity.CashFree = available
		b.tradeMsg.Equity.Lock.Unlock()
		b.cb.OnEquity(time.Now().UnixMicro())
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warnf("get equity err %v", err)
	}
}

func (b *AcxfinanceUsdtSwap) getFees() {
	url := "/api/v1/account/commissionRate"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.Get("data")
		makerCommissionRate := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("makerCommissionRate")))
		takerCommissionRate := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("takerCommissionRate")))
		b.makerFee.Store(makerCommissionRate)
		b.takerFee.Store(takerCommissionRate)
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warnf("get fees err %v", err)
	}
}

// setLeverRate 设置账户的杠杆参数 不需要返回值
func (b *AcxfinanceUsdtSwap) setLeverRate() {
	// 基本请求信息
	url := "/api/v1/trade/setLeverage"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	params["leverage"] = 20
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	// 发起请求
	err = b.call(http.MethodPost, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		if handlerErr != nil {
			return
		}
		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
		}
	})
	if err != nil || handlerErr != nil {
		log.Warn("set leverRate fail %v, %v", err, handlerErr)
	}
}

// 撤掉所有挂单
func (b *AcxfinanceUsdtSwap) cancelAllOpenOrders(only bool) {
	url := "/api/v1/trade/openOrders"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received single openorder value: %s\n", value)
		if handlerErr != nil {
			return
		}

		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}
		datas := value.GetArray("data")
		for _, data := range datas {
			symbol := string(data.GetStringBytes("symbol"))
			oid := string(data.GetStringBytes("orderId"))
			cid := string(data.GetStringBytes("clientOrderId"))
			b.cancelAllOrderID(symbol, oid, cid)
		}
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warn("cancel open order err %v", err)
	}
}

// cancelOrderID 撤单 用自己传入的symbol 而不是实例的symbol
func (b *AcxfinanceUsdtSwap) cancelAllOrderID(symbol string, oid string, cid string) {
	url := "/api/v1/trade/cancelOrder"
	params := make(map[string]interface{})
	params["symbol"] = symbol
	params["orderId"] = oid
	params["clientOrderId"] = cid
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	start := time.Now().UnixMilli()
	pass := int64(0)

	err = b.call(http.MethodPost, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start

		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received respBody: %s\n", respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			return
		}

		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warn("cancel order by id err %v", err)
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.cancelOrderPass.Update(pass)
	}
}

// 清空所有持仓
func (b *AcxfinanceUsdtSwap) cleanPosition(only bool) {
	url := "/api/v1/account/positions"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	// 发起请求
	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		if handlerErr != nil {
			return
		}
		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}
		datas := value.GetArray("data")

		for _, data := range datas {
			left := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("positionAmt")))
			symbol := string(data.GetStringBytes("symbol"))
			left_params := make(map[string]interface{})
			left_params["symbol"] = symbol
			left_params["type"] = "MARKET"
			left_params["timeInForce"] = "GTC"
			left_params["priceProtect"] = true
			if left > 0 {
				left_params["side"] = "SELL"
				left_params["quantity"] = left
				err = b.call(http.MethodPost, "/api/v1/trade/order", left_params, true, nil)
				if err != nil {
					//得检查是否有限频提示
					//fmt.Printf("close long pos err %v", err)
					log.Warn("close long position err %v", err)
				}
			} else if left < 0 {
				left_params["side"] = "BUY"
				left_params["quantity"] = -left
				err = b.call(http.MethodPost, "/api/v1/trade/order", left_params, true, nil)
				if err != nil {
					//得检查是否有限频提示
					log.Warn("close short position err %v", err)
				}
			}
		}
	})
}

// adjustAcct 开始交易前 把账户调整到合适的状态 包括调整杠杆 仓位模式 买入一定数量的平台币等等
func (b *AcxfinanceUsdtSwap) adjustAcct() {
	// step 1
	b.setLeverRate()
}

// BeforeTrade 开始交易前需要做的所有工作 调整好杠杆
func (b *AcxfinanceUsdtSwap) BeforeTrade(mode helper.BeforeTradeMode) {
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
	b.cancelAllOpenOrders(true)
	b.cleanPosition(true)
	b.adjustAcct()
	// 获取账户资金
	b.getEquity()
	b.getFees()
	log.Infof("[rest] maker手续费 %v taker手续费 %v", b.makerFee.Load(), b.takerFee.Load())
	// 获取ticker
	b.getTicker()
}

// AfterTrade 结束交易时需要做的所有工作  清空挂单和仓位
func (b *AcxfinanceUsdtSwap) AfterTrade(mode helper.AfterTradeMode) bool {
	var isleft bool
	switch mode {
	case helper.AfterTradeModePrepare:
	case helper.AfterTradeModeCloseOne:
		b.cancelAllOpenOrders(true)
		//isleft = b.cleanPosition(true)
		b.cleanPosition(true)
	case helper.AfterTradeModeCloseAll:
		b.cancelAllOpenOrders(false)
		//isleft = b.cleanPosition(false)
		b.cleanPosition(false)
	}
	return isleft
}

// getPosition 获取指定币种的仓位 获取之后触发回调
func (b *AcxfinanceUsdtSwap) getPosition() {
	url := "/api/v1/account/positions"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	// 发起请求
	err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		if handlerErr != nil {
			return
		}
		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}
		datas := value.GetArray("data")

		b.tradeMsg.Position.Reset()
		for _, data := range datas {
			symbol := string(data.GetStringBytes("symbol"))
			if symbol != b.symbol {
				continue
			}
			size, nerr := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("positionAmt")))
			if nerr != nil {
				log.Errorf("acxfinance size %v", err)
			}
			var side int64
			//fmt.Print("SIZE", size.Sign())

			if size.Sign() < 0 {
				side = 0
			} else {
				side = 1
			}
			price := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("openPrice")))
			b.tradeMsg.Position.Lock.Lock()
			if side == 0 {
				b.tradeMsg.Position.ShortPos = size
				b.tradeMsg.Position.ShortAvg = price
			} else {
				b.tradeMsg.Position.LongPos = size
				b.tradeMsg.Position.LongAvg = price
			}
			b.tradeMsg.Position.Time = time.Now().UnixMilli()
			b.tradeMsg.Position.Lock.Unlock()
		}
		b.cb.OnPosition(time.Now().UnixMicro())
	})

	if err != nil {
		log.Warn("get position err %v", err)
	}
}

// takeOrder 下单
func (b *AcxfinanceUsdtSwap) takeOrder(price float64, size decimal.Decimal, cid string, side helper.OrderSide, orderType helper.OrderType, t int64) {
	var url string
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	params["clientOrderId"] = cid
	params["quantity"] = size
	if side == helper.OrderSideKD || side == helper.OrderSidePK {
		params["side"] = "BUY"
	} else {
		params["side"] = "SELL"
	}

	if orderType == helper.OrderTypeLimit {
		url = "/api/v1/trade/order"
		params["type"] = "LIMIT"
		params["timeInForce"] = "GTC"
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
	} else if orderType == helper.OrderTypeIoc {
		url = "/api/v1/trade/order"
		params["effect_type"] = 2
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
	} else if orderType == helper.OrderTypePostOnly {
		url = "/api/v1/trade/order"
		params["type"] = "POSTONLY"
		params["timeInForce"] = "GTC"
		params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
	} else if orderType == helper.OrderTypeMarket {
		url = "/api/v1/trade/order"
		params["type"] = "MARKET"
	} else {
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
	var handlerErr error
	var err error
	var value *fastjson.Value

	start := time.Now().UnixMilli()
	pass := int64(0)

	b.systemPass.Update(time.Now().UnixMicro() - t)

	err = b.call(http.MethodPost, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received respBody: %s\n", respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			// 出现错误的时候也要触发回调, 不是json格式
			var order helper.OrderEvent
			order.Type = helper.OrderEventTypeERROR
			order.ClientID = cid
			b.cb.OnOrder(time.Now().UnixMicro(), order)
			log.Errorf("[%s]%s下单失败 %s", b.exchangeName, cid, handlerErr.Error())
			return
		}

		code := value.GetInt64("code")
		if code != 0 {
			// 出现错误的时候也要触发回调
			var order helper.OrderEvent
			order.Type = helper.OrderEventTypeERROR
			order.ClientID = cid
			b.cb.OnOrder(time.Now().UnixMicro(), order)
			log.Errorf("[%s]%s下单失败 %s", b.exchangeName, cid, helper.BytesToString(respBody))
			if code == 203 {
				// 出现限频
				log.Errorf("[%s]已被限频 %s", b.exchangeName, helper.BytesToString(respBody))
				b.cb.OnMsg(fmt.Sprintf("[%s]已被限频 %s", b.exchangeName, helper.BytesToString(respBody)))
				b.cb.OnExit(fmt.Sprintf("已被限频:%s", helper.BytesToString(respBody)))
			}

			return
		}

		data := value.Get("data")
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeNEW
		order.OrderID = string(data.GetStringBytes("orderId"))
		order.ClientID = string(data.GetStringBytes("clientOrderId"))
		if len(order.OrderID) == 0 || len(order.ClientID) == 0 {
			// 此时不能返回 err， 因为订单可能被成交
			//var order helper.OrderEvent
			//order.Type = helper.OrderEventTypeERROR
			//order.ClientID = cid
			//b.cb.OnOrder(time.Now().UnixMicro(), order)
			log.Errorf("[%s]%s下单失败 orderid或clientid为空 %s", b.exchangeName, cid, helper.BytesToString(respBody))
			return
		}

		//fmt.Printf("PLACE ORDER %+v\n", order)
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		//go b.checkOrderID(order.OrderID, order.ClientID)
	})

	if err != nil {
		log.Errorf("[%s]%s下单失败 %s", b.exchangeName, cid, err.Error())
		// 只处理 timeout 情况，这是由于对手服务器可能的问题
		if err.Error() == "timeout" {
			log.Errorf("%s]%s 自动查单 catch", b.exchangeName, cid)
			go b.checkOrderID("", cid)
			//go b.cancelOrderID("", cid)
			return
		}
		//得检查是否有限频提示
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeERROR
		order.ClientID = cid
		b.cb.OnOrder(time.Now().UnixMicro(), order)
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.takerOrderPass.Update(pass)
	}
}

// cancelOrderID 撤单 acxfinance 仅支持oid撤单
func (b *AcxfinanceUsdtSwap) cancelOrderID(oid string, cid string) {
	url := "/api/v1/trade/cancelOrder"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	if len(oid) != 0 {
		params["orderId"] = oid
	}
	params["clientOrderId"] = cid
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	start := time.Now().UnixMilli()
	pass := int64(0)

	err = b.call(http.MethodPost, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start

		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received respBody: %s\n", respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			return
		}

		code := value.GetInt64("code")
		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}

		data := value.Get("data")
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeNEW

		order.OrderID = string(data.GetStringBytes("orderId"))
		order.ClientID = string(data.GetStringBytes("clientOrderId"))

		//fmt.Printf("CANCEL ORDER %+v\n", order)
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		//go b.checkOrderID(order.OrderID, order.ClientID)
	})

	if err != nil {
		//得检查是否有限频提示
		log.Warnf("cancel order by id err %v", err)
		if err.Error() == "timeout" {
			log.Warn("catch timeout")
		}
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.cancelOrderPass.Update(pass)
	}
}

// checkOrder 查单 acxfinance仅支持oid查单
func (b *AcxfinanceUsdtSwap) checkOrderID(oid string, cid string) {
	url := "/api/v1/trade/order"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	if len(oid) != 0 {
		params["orderId"] = oid
	}
	params["clientOrderId"] = cid
	// 必备变量
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Received respBody: %s\n", respBody)
		//fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			return
		}

		code := value.GetInt64("code")

		if code != 0 {
			handlerErr = errors.New(helper.BytesToString(respBody))
			return
		}

		data := value.Get("data")
		var order helper.OrderEvent
		if data.Type() == fastjson.TypeNull {
		} else {
			// 查到订单
			filled, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("executedQty")))
			avePrice := decimal.Zero
			if filled.GreaterThan(decimal.Zero) { //空 多 都一样？！
				avePrice, _ = decimal.NewFromString(helper.BytesToString(data.GetStringBytes("avgPrice")))
				dealValue, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("cumQuote")))
				takerFee := decimal.NewFromFloat(b.takerFee.Load())
				cashFee := dealValue.Mul(takerFee)
				order.CashFee = cashFee
				//scaled := cashFee.Mul(decimal.NewFromInt(100)) // Scale value by 100
				//rounded := scaled.Ceil()                       // Round up to nearest whole number
				//order.CashFee = rounded.Div(decimal.NewFromInt(100))
			}
			status := helper.BytesToString(data.GetStringBytes("status"))

			switch status {
			case "NEW":
				order.Type = helper.OrderEventTypeNEW
			case "FILLED", "CANCELED":
				order.Type = helper.OrderEventTypeREMOVE
			case "PARTIALLY_FILLED":
				order.Type = helper.OrderEventTypeNEW
				order.PartTraded = true
			default:
				order.Type = helper.OrderEventTypeNEW
			}
			order.OrderID = string(data.GetStringBytes("orderId"))
			order.ClientID = string(data.GetStringBytes("clientOrderId"))
			order.FilledPrice = avePrice.InexactFloat64()
			order.Filled = filled
			b.cb.OnOrder(time.Now().UnixMicro(), order)
		}
	})

	if err != nil {
		log.Warn("check order by id err %v", err)
		if err.Error() == "timeout" {
			log.Warn("check timeout catch! ")
			//go func() {
			//	time.Sleep(time.Second)
			//	b.checkOrderID("", cid)
			//}()
		}
	}
}

// call 专用于acxfinance_usdt_swap的发起http请求函数
func (b *AcxfinanceUsdtSwap) call(reqMethod string, reqUrl string, params map[string]interface{}, needSign bool, respHandler rest.FastHttpRespHandler) error {
	requestHeaders := make(map[string]string, 0)
	encodes := ""
	encode := make([]string, 0)
	for key, param := range params {
		encode = append(encode, fmt.Sprintf("%s=%v", key, param))
	}
	if needSign {
		if params == nil {
			params = make(map[string]interface{}, 0)
		}
		now := time.Now().UnixNano() / int64(time.Millisecond)
		sort.Strings(encode)
		encodes = strings.Join(encode, "&")
		var sign string
		specialurl := "app-api.acx.finance"
		switch reqMethod {
		case http.MethodGet:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, specialurl, reqUrl, strconv.FormatInt(now, 10), b.params.AccessKey, params)
		case http.MethodPost:
			sign = createSignaturePost(b.params.SecretKey, reqMethod, specialurl, reqUrl, strconv.FormatInt(now, 10), b.params.AccessKey)
		}
		requestHeaders["ACX-SIGNATURE"] = sign
		requestHeaders["ACX-API-KEY"] = b.params.AccessKey
		requestHeaders["ACX-TIMESTAMP"] = strconv.FormatInt(now, 10)
	} else {
		encodes = strings.Join(encode, "&")
	}
	requestHeaders["Content-Type"] = "application/json"
	var body map[string]interface{}
	s := strings.Builder{}
	s.WriteString(BaseUrl + reqUrl)
	switch reqMethod {
	case http.MethodGet, http.MethodDelete:
		if len(encodes) > 0 {
			if strings.Contains(reqUrl, "?") {
				s.WriteString("&")
			} else {
				s.WriteString("?")
			}
			s.WriteString(encodes)
		}
	case http.MethodPost:
		body = params

	default:
	}

	orderParamsBytes, _ := json.Marshal(body)
	requestBody := bytes.NewBuffer(orderParamsBytes)
	requestBodyBytes := requestBody.Bytes()

	status, err := b.client.Request(reqMethod, s.String(), requestBodyBytes, requestHeaders, respHandler)

	if err != nil {
		//fmt.Printf("Call Func Err %v", err)
		b.failNum.Add(1)
		if b.failNum.Load() > 1000 {
			b.cb.OnExit("连续request请求出错 需要停机")
		}
		return err
	}
	if status != 200 {
		b.failNum.Add(1)
		if b.failNum.Load() > 1000 {
			b.cb.OnExit("连续返回值出错 需要停机")
		}
		return fmt.Errorf("status:%d", status)
	}
	b.failNum.Store(0)
	return nil
}

func createSignaturePost(apiSecret string, method string, host string, path string, timestamp string, apiKey string) string {
	message := fmt.Sprintf("%s\n%s\n%s\n%s\n%s", method, host, path, timestamp, apiKey)

	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return signature
}

func createSignatureGet(apiSecret string, method string, host string, path string, timestamp string, apiKey string, params map[string]interface{}) string {
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var strParams string

	for _, k := range keys {
		v, ok := params[k].(string)
		if !ok {
			continue
		}
		strParams += fmt.Sprintf("%s=%s&", k, url.QueryEscape(v))
	}
	strParams = strings.TrimRight(strParams, "&")

	var message string
	if len(params) == 0 {
		message = fmt.Sprintf("%s\n%s\n%s\n%s\n%s", method, host, path, timestamp, apiKey)
	} else {
		message = fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s", method, host, path, timestamp, apiKey, strParams)
	}
	//fmt.Println(message)

	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return signature
}

func (b *AcxfinanceUsdtSwap) GetDelay() (int64, int64, int64) {
	return b.takerOrderPass.GetDelay(), b.cancelOrderPass.GetDelay(), b.systemPass.GetDelay()
}

// 新写了一个func代替
func (b *AcxfinanceUsdtSwap) GetFee() (float64, float64) {
	return b.takerFee.Load(), b.makerFee.Load()
}

// SendSignal 发送信号 关键函数 必须要异步发单
func (b *AcxfinanceUsdtSwap) SendSignal(signals []helper.Signal) {
	for _, s := range signals {
		log.Debugf("发送信号 %s", s)
		switch s.Type {
		case helper.SignalTypeNewOrder:
			go b.takeOrder(s.Price, s.Amount, s.ClientID, s.OrderSide, s.OrderType, s.Time)
		case helper.SignalTypeCancelOrder:
			//if s.OrderID != "" {
			//	go b.cancelOrderID(s.OrderID, s.ClientID)
			//}
			go b.cancelOrderID(s.OrderID, s.ClientID)
		case helper.SignalTypeCheckOrder:
			go b.checkOrderID(s.OrderID, s.ClientID)
		case helper.SignalTypeGetPos:
			go b.getPosition()
		case helper.SignalTypeGetEquity:
			go b.getEquity()
		}
	}
}
