package dydx_usdc_swap

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"
	"zbeast/pkg/quant/broker/client/rest"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"
	"zbeast/third/starkex"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

const (
	BaseUrl = "https://api.dydx.exchange"
)

var (
	handyPool fastjson.ParserPool
)

type DydxUsdcSwap struct {
	params       *helper.BrokerConfig
	pair         helper.Pair
	symbol       string
	client       *rest.Client
	cb           helper.CallbackFunc
	tradeMsg     *helper.TradeMsg
	exchangeName helper.BrokerName
	exchangeInfo map[string]helper.ExchangeInfo
	failNum      atomic.Int64

	pairInfo        *helper.ExchangeInfo
	takerOrderPass  helper.PassTime
	cancelOrderPass helper.PassTime
	systemPass      helper.PassTime
	takerFee        atomic.Float64
	makerFee        atomic.Float64
}

func NewRs(params *helper.BrokerConfig, msg *helper.TradeMsg, pairInfo *helper.ExchangeInfo, cb helper.CallbackFunc) *DydxUsdcSwap {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}

	b := &DydxUsdcSwap{
		client:       rest.NewClient(params.ProxyURL, params.LocalAddr),
		params:       params,
		exchangeName: helper.BrokernameDydxUsdcSwap,
		pair:         params.Pair,
		cb:           cb,
		pairInfo:     pairInfo,
		exchangeInfo: map[string]helper.ExchangeInfo{},
	}
	b.symbol = pairToSymbol(b.pair)
	b.tradeMsg = msg
	return b
}

func pairToSymbol(pair helper.Pair) string {
	return strings.ToUpper(pair.Base + "-" + strings.Replace(pair.Quote, "usdc", "usd", -1))
}

func (b *DydxUsdcSwap) getExchangeInfo() []*helper.ExchangeInfo {
	url := "/v3/markets/"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var value *fastjson.Value
	infos := make([]*helper.ExchangeInfo, 0)
	err := b.call(http.MethodGet, url, nil, false, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		var handlerErr error
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Recieve value: %s\n", value)
		if handlerErr != nil {
			b.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机 %s", b.exchangeName, handlerErr.Error()))
			return
		}
		data := value.GetObject("markets")

		data.Visit(func(key []byte, v *fastjson.Value) {
			symbol := helper.BytesToString(v.GetStringBytes("market"))
			enableTrade := string(v.GetStringBytes("status")) == "ONLINE"
			minSize, err := decimal.NewFromString(string(v.GetStringBytes("minOrderSize")))
			if err != nil {
				log.Errorf("dydx minSize err:%v", err)
				return
			}
			tickSize, _ := decimal.NewFromString(string(v.GetStringBytes("tickSize")))
			stepSize, _ := decimal.NewFromString(string(v.GetStringBytes("stepSize")))
			info := helper.ExchangeInfo{
				Pair:           helper.Pair{Base: string(v.GetStringBytes("baseAsset")), Quote: string(v.GetStringBytes("quoteAsset"))},
				Symbol:         symbol,
				Status:         enableTrade,
				TickSize:       tickSize,
				StepSize:       stepSize,
				MaxOrderAmount: decimal.NewFromFloat(100000000000),
				MinOrderAmount: minSize,
				MaxOrderValue:  decimal.NewFromFloat(30000),
				MinOrderValue:  decimal.NewFromFloat(10),
			}
			infos = append(infos, &info)
			if symbol == b.symbol {
				b.pairInfo.Pair = b.pair
				b.pairInfo.TickSize = info.TickSize
				b.pairInfo.StepSize = info.StepSize
				b.pairInfo.MaxOrderValue = info.MaxOrderValue
				b.pairInfo.MinOrderValue = info.MinOrderValue
				b.pairInfo.MaxOrderAmount = info.MaxOrderAmount
				b.pairInfo.MinOrderAmount = info.MinOrderAmount
				b.pairInfo.Status = info.Status
			}
		})
	})

	if err != nil {
		b.cb.OnExit(fmt.Sprintf("[%s] 获取交易信息失败 需要停机. %s", b.exchangeName, err.Error()))
		return nil
	}
	return infos
}

func (b *DydxUsdcSwap) getTicker() {
	url := "/v3/orderbook/"
	params := make(map[string]interface{})
	params["symbol"] = b.symbol
	p := handyPool.Get()
	defer handyPool.Put(p)
	var err error
	var value *fastjson.Value
	var handlerErr error
	err = b.call(http.MethodGet, url, params, false, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Recieve value: %s\n", value)
		if handlerErr != nil {
			return
		}
		ask := value.GetArray("asks")[0]
		bid := value.GetArray("bids")[0]
		ap := fastfloat.ParseBestEffort(string(ask.GetStringBytes("price")))
		bp := fastfloat.ParseBestEffort(string(bid.GetStringBytes("price")))

		b.tradeMsg.Ticker.Ap.Store(ap)
		b.tradeMsg.Ticker.Bp.Store(bp)
		b.tradeMsg.Ticker.Mp.Store(b.tradeMsg.Ticker.Price())
		b.cb.OnTicker(time.Now().UnixMicro())
	})
	if err != nil {
		log.Warnf("get ticker err %v", err)
	}
}

func (b *DydxUsdcSwap) getEquity() {
	url := "/v3/accounts"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	//err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Recieve value: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.GetArray("accounts")[0]
		available := fastfloat.ParseBestEffort(string(data.GetStringBytes("freeCollateral")))
		equity := fastfloat.ParseBestEffort(string(data.GetStringBytes("equity")))
		b.tradeMsg.Equity.Lock.Lock()
		b.tradeMsg.Equity.Cash = equity
		b.tradeMsg.Equity.CashFree = available
		b.tradeMsg.Equity.Lock.Unlock()
		b.cb.OnEquity(time.Now().UnixMicro())
	})
	if err != nil {
		log.Warnf("get equity err %v", err)
	}
}

func (b *DydxUsdcSwap) getFees() {
	url := "/v3/users"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		//fmt.Printf("Receive fees value: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.Get("user")
		makerFee := fastfloat.ParseBestEffort(string(data.GetStringBytes("makerFeeRate")))
		takerFee := fastfloat.ParseBestEffort(string(data.GetStringBytes("takerFeeRate")))
		b.makerFee.Store(makerFee)
		b.takerFee.Store(takerFee)
	})
	if err != nil {
		log.Warnf("get fees err %v", err)
	}
}

// lack of take order
func (b *DydxUsdcSwap) cleanAllPositions(only bool) {
	url := "/v3/positions"
	params := make(map[string]interface{})
	params["status"] = "OPEN"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive all positions value: %s\n", value)
		if handlerErr != nil {
			return
		}
	})
	if err != nil {
		log.Warnf("get all positions err %v", err)
	}
}

// lack of take order
func (b *DydxUsdcSwap) cleanOnePosition(only bool) {
	url := "/v3/positions"
	params := make(map[string]interface{})
	params["market"] = b.symbol
	params["status"] = "OPEN"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive all positions value: %s\n", value)
		if handlerErr != nil {
			return
		}
	})
	if err != nil {
		log.Warnf("get all positions err %v", err)
	}
}

func (b *DydxUsdcSwap) cancelAllOpenOrders(only bool) {
	url := "/v3/orders"
	p := handyPool.Get()
	defer handyPool.Put(p)
	params := make(map[string]interface{})
	params["market"] = "ETH-USD"
	var handlerErr error
	var err error
	var value *fastjson.Value

	start := time.Now().UnixMilli()
	pass := int64(0)

	err = b.call(http.MethodDelete, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive cancel all open order: %s\n", value)
		if handlerErr != nil {
			return
		}
	})
	if err != nil {
		log.Warnf("cancel all open orders err %v", err)
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.cancelOrderPass.Update(pass)
	}

}

func (b *DydxUsdcSwap) cancelOneOpenOrder(only bool) {
	url := "/v3/active-orders"
	params := make(map[string]interface{})
	params["market"] = b.symbol
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodDelete, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive cancel all open order: %s\n", value)
		if handlerErr != nil {
			return
		}
	})
	if err != nil {
		log.Warnf("cancel all open orders err %v", err)
	}

}

// Description: Get active (not filled or canceled) orders for a user by specified parameters.
func (b *DydxUsdcSwap) getActiveOrders(only bool) {
	//url := "/v3/orders"
	url := "/v3/active-orders"
	params := make(map[string]interface{})
	params["market"] = b.symbol
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive all orders: %s\n", value)
		if handlerErr != nil {
			return
		}
	})
	if err != nil {
		log.Warnf("cancel all orders err %v", err)
	}
}

func (b *DydxUsdcSwap) getAllFillsOrders() {
	url := "/v3/fills"
	//url := "/v3/active-orders"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive fills orders: %s\n", value)
		if handlerErr != nil {
			return
		}

	})
	if err != nil {
		log.Warnf("get fills orders err %v", err)
	}

}

func (b *DydxUsdcSwap) getFillsOrders(oid string) (decimal.Decimal, decimal.Decimal, decimal.Decimal) {
	url := "/v3/fills"
	//url := "/v3/active-orders"
	params := make(map[string]interface{})
	params["orderId"] = oid
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	var filledPrice, filledSize, filledFee decimal.Decimal

	err = b.call(http.MethodGet, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive fills orders: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.GetArray("fills")[0]
		filledPrice, _ = decimal.NewFromString(helper.BytesToString(data.GetStringBytes("price")))
		filledSize, _ = decimal.NewFromString(helper.BytesToString(data.GetStringBytes("size")))
		filledFee, _ = decimal.NewFromString(helper.BytesToString(data.GetStringBytes("fee")))
		//return filledPrice, filledSize, filledFee

	})
	if err != nil {
		log.Warnf("get fills orders err %v", err)
	}
	return filledPrice, filledSize, filledFee

}

// check order by oid or cid
func (b *DydxUsdcSwap) checkOrderByID(cid string, oid string) {
	url := "/v3/orders/"
	//params := make(map[string]interface{})
	//params["symbol"] = b.symbol
	if len(oid) != 0 {
		url = url + oid
	} else if len(cid) != 0 {
		url = "/v3/orders/client/" + cid
	} else {
		return
	}
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = b.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		log.Infof("##Rest check order data %v\n", value)
		fmt.Printf("Receive check order by ID: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.Get("order")
		symbol := helper.BytesToString(data.GetStringBytes("market"))
		if symbol != b.symbol {
			return
		}
		var order helper.OrderEvent
		order.OrderID = string(data.GetStringBytes("id"))
		order.ClientID = string(data.GetStringBytes("clientId"))
		status := helper.BytesToString(data.GetStringBytes("status"))
		switch status {
		case "FILLED", "CANCELED":
			order.Type = helper.OrderEventTypeREMOVE
			size, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("size")))
			remainsize, _ := decimal.NewFromString(helper.BytesToString(data.GetStringBytes("remainingSize")))
			filled := size.Sub(remainsize)
			order.Filled = filled
			if !order.Filled.IsZero() {
				order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("price")))
				//order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
			}
		default:
			order.Type = helper.OrderEventTypeNEW
		}
		fmt.Printf("@@@@@@REST CHECK:oid:%v@@cid:%v@@order.Type:%v", order.OrderID, order.ClientID, order.Type)
		fmt.Printf("@@@@@@rest check order status: %s\n", status)
		fmt.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@check order oid:%v##cid:%v", order.OrderID, order.ClientID)
		b.cb.OnOrder(time.Now().UnixMicro(), order)
	})
	if err != nil {
		log.Warnf("check order by id err %v", err)
	}
}

func (b *DydxUsdcSwap) cancelOrderByID(oid string) {
	url := "/v3/orders/"
	url = url + oid

	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value
	start := time.Now().UnixMilli()
	pass := int64(0)

	err = b.call(http.MethodDelete, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start
		value, handlerErr = p.ParseBytes(respBody)
		log.Infof("!!Rest cancel order data %v\n", value)
		fmt.Printf("Receive cancel order by ID: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.Get("cancelOrder")
		fmt.Printf("rest cancel data:%v\n", data)

	})
	if err != nil {
		log.Warnf("@@@@@cancel order by id err %v", err)
		//b.checkOrderByID(cid, oid)
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.cancelOrderPass.Update(pass)
	}
}

func ExpireAfter(duration time.Duration) string {
	return time.Now().Add(duration).UTC().Format("2006-01-02T15:04:05.000Z")
}

func (b *DydxUsdcSwap) takeOrder(price float64, size decimal.Decimal, cid string, side helper.OrderSide, orderType helper.OrderType, t int64) {
	var url string
	params := make(map[string]interface{})
	params["market"] = b.symbol
	params["clientId"] = cid
	params["size"] = size
	var starkside string
	if side == helper.OrderSideKD || side == helper.OrderSidePK {
		params["side"] = "BUY"
		starkside = "BUY"
	} else {
		params["side"] = "SELL"
		starkside = "SELL"
	}

	params["price"] = helper.FixPrice(price, b.pairInfo.TickSize).String()
	starkprice := helper.FixPrice(price, b.pairInfo.TickSize).String()
	switch orderType {
	case helper.OrderTypeLimit:
		url = "/v3/orders"
		params["type"] = "LIMIT"
		params["postOnly"] = false

	case helper.OrderTypePostOnly:
		url = "/v3/orders"
		params["type"] = "LIMIT"
		params["postOnly"] = true

	}

	starkparam := starkex.OrderSignParam{
		NetworkId:  1,
		PositionId: 337234,
		Market:     b.symbol,
		Side:       starkside,
		HumanSize:  size.String(),
		HumanPrice: starkprice,
		LimitFee:   "10.1",
		ClientId:   cid,
		Expiration: ExpireAfter(60 * time.Minute),
	}

	starkPrivateKey := "0315864e21e7a4b50305c002bdea45aaccd0578938ffe328c4a83ea8cfd29ffc"
	//starkPrivateKey := b.params.StarkPrivateKey

	// 3. Call SignOrder method
	//fmt.Printf("@@@stark private:%v\n", b.params.StarkPrivateKey)
	//fmt.Printf("&&&&&&stark private:%v\n", starkPrivateKey)
	//fmt.Printf("&&&&&&&&&&&&&stark params:%v\n", starkparam)

	signature, starkerr := starkex.OrderSign(starkPrivateKey, starkparam)
	//fmt.Printf("&&&&&&&&&&&&&sign result:%v@@err:%v\n", signature, starkerr)

	//signature, starkerr := signer.SignOrder(starkparam)
	if starkerr != nil {
		fmt.Println("Error signing the order:", starkerr)
		return
	}

	//fmt.Println("Signature:", signature)

	params["timeInForce"] = "GTT"
	params["limitFee"] = "10.1"
	params["expiration"] = ExpireAfter(60 * time.Minute)
	params["signature"] = signature

	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	start := time.Now().UnixMilli()
	pass := int64(0)
	b.systemPass.Update(time.Now().UnixMicro() - t)

	err = b.postcall2(http.MethodPost, url, params, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		pass = time.Now().UnixMilli() - start
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Receive takeorder value: %s\n", value)
		if handlerErr != nil {
			var order helper.OrderEvent
			order.Type = helper.OrderEventTypeERROR
			order.ClientID = cid
			b.cb.OnOrder(time.Now().UnixMicro(), order)
			log.Errorf("[%s]%s 下单失败 %s", b.exchangeName, cid, handlerErr.Error())
			return
		}
		log.Infof("@@Rest take order data %v\n", value)
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeNEW
		data := value.Get("order")
		order.OrderID = string(data.GetStringBytes("id"))
		order.ClientID = string(data.GetStringBytes("clientId"))
		fmt.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@take order oid:%v##cid:%v", order.OrderID, order.ClientID)
		b.cb.OnOrder(time.Now().UnixMicro(), order)
	})

	if err != nil {
		var order helper.OrderEvent
		order.Type = helper.OrderEventTypeERROR
		order.ClientID = cid
		b.cb.OnOrder(time.Now().UnixMicro(), order)
		log.Errorf("[%s]%s 下单失败 %s", b.exchangeName, cid, err.Error())
	}
	if err == nil && handlerErr == nil && pass > 0 {
		b.takerOrderPass.Update(pass)
	}
}

func (b *DydxUsdcSwap) call(reqMethod string, reqUrl string, params map[string]interface{}, needSign bool, respHandler rest.FastHttpRespHandler) error {
	requestHeaders := make(map[string]string, 0)
	encodes := ""
	encode := make([]string, 0)
	if needSign {
		for key, param := range params {
			encode = append(encode, fmt.Sprintf("%s=%v", key, param))
		}
	} else {
		for _, param := range params {
			encode = append(encode, fmt.Sprintf("%v", param))
		}
	}

	if needSign {
		if params == nil {
			params = make(map[string]interface{}, 0)
		}
		now := time.Now().UTC()
		timestamp := now.Format(time.RFC3339)
		encodes = strings.Join(encode, "&")
		var sign string
		switch reqMethod {
		case http.MethodGet:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes)
		case http.MethodPost:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes)
		case http.MethodDelete:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes)
		}
		requestHeaders["DYDX-SIGNATURE"] = sign
		requestHeaders["DYDX-API-KEY"] = b.params.AccessKey
		requestHeaders["DYDX-PASSPHRASE"] = b.params.PassKey
		requestHeaders["DYDX-TIMESTAMP"] = timestamp

	} else {
		encodes = strings.Join(encode, "&")
	}
	var body map[string]interface{}
	s := strings.Builder{}
	s.WriteString(BaseUrl + reqUrl)
	switch reqMethod {
	case http.MethodGet, http.MethodDelete, http.MethodPost:
		if len(encodes) > 0 {
			if strings.Contains(reqUrl, "?") {
				s.WriteString("&")
			}
			if needSign {
				s.WriteString("?")
			}
			s.WriteString(encodes)
		}
	default:

	}

	orderParamsBytes, _ := json.Marshal(body)
	requestBody := bytes.NewBuffer(orderParamsBytes)
	requestBodyBytes := requestBody.Bytes()

	status, err := b.client.Request(reqMethod, s.String(), requestBodyBytes, requestHeaders, respHandler)

	if err != nil {
		fmt.Printf("Call Func Err %v", err)
		b.failNum.Add(1)
		if b.failNum.Load() > 50 {
			b.cb.OnExit("连续请求出错 需要停机")
		}
		return err
	}

	if status != 200 {
		b.failNum.Add(1)
		if b.failNum.Load() > 50 {
			b.cb.OnExit("连续请求出错 需要停机")
		}
		return fmt.Errorf("status: %d", status)

	}
	b.failNum.Store(0)
	return nil

}

func (b *DydxUsdcSwap) postcall2(reqMethod string, reqUrl string, params map[string]interface{}, needSign bool, respHandler rest.FastHttpRespHandler) error {
	requestHeaders := make(map[string]string, 0)
	encodes := ""
	//encode := make([]string, 0)
	//fmt.Print(encode)

	if needSign {
		if params == nil {
			params = make(map[string]interface{}, 0)
		}
		orderParamsBytes, _ := json.Marshal(params)
		now := time.Now().UTC()
		timestamp := now.Format(time.RFC3339)
		var sign string
		//fmt.Print(sign)
		switch reqMethod {
		case http.MethodGet:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes)
		case http.MethodDelete:
			sign = createSignatureGet(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes)
		case http.MethodPost:
			sign = createSignaturePost2(b.params.SecretKey, reqMethod, reqUrl, reqUrl, timestamp, b.params.AccessKey, encodes, string(orderParamsBytes))
		}
		requestHeaders["DYDX-SIGNATURE"] = sign
		requestHeaders["DYDX-API-KEY"] = b.params.AccessKey
		requestHeaders["DYDX-PASSPHRASE"] = b.params.PassKey
		requestHeaders["DYDX-TIMESTAMP"] = timestamp
		requestHeaders["Content-Type"] = "application/json"
		requestHeaders["User-Agent"] = "go-dydx"

	}
	s := strings.Builder{}
	s.WriteString(BaseUrl + reqUrl)
	switch reqMethod {
	case http.MethodGet, http.MethodDelete:
		if len(encodes) > 0 {
			if strings.Contains(reqUrl, "?") {
				s.WriteString("&")
			}
			if needSign {
				s.WriteString("?")
			}
			s.WriteString(encodes)
		}
	default:
	}

	orderParamsBytes, _ := json.Marshal(params)
	requestBody := bytes.NewBuffer(orderParamsBytes)
	requestBodyBytes := requestBody.Bytes()

	//status, err := b.client.Request(reqMethod, s.String(), string(orderParamsBytes), requestHeaders, respHandler)
	status, err := b.client.Request(reqMethod, s.String(), requestBodyBytes, requestHeaders, respHandler)

	if err != nil {
		fmt.Printf("Call Func Err %v", err)
		b.failNum.Add(1)
		if b.failNum.Load() > 50 {
			b.cb.OnExit("连续请求出错 需要停机")
		}
		return err
	}

	if status != 201 {
		b.failNum.Add(1)
		if b.failNum.Load() > 50 {
			b.cb.OnExit("连续请求出错 需要停机")
		}
		return fmt.Errorf("status: %d", status)
	}
	b.failNum.Store(0)
	return nil

}

// func createSignatureGet(apiSecret string, method string, host string, path string, ts string, apiKey string, params map[string]interface{}) string {
func createSignatureGet(apiSecret string, method string, host string, path string, ts string, apiKey string, encodes string) string {
	var message string

	if len(encodes) == 0 {
		message = ts + method + path
		//message = fmt.Sprintf("%s\n%s\n%s\n%s\n%s", method, host, path, timestamp, apiKey)
	} else {
		message = ts + method + path + "?" + encodes
	}

	key, err := base64.URLEncoding.DecodeString(apiSecret)
	if err != nil {
		panic(err)
	}

	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))

	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))
	return signature
}

func createSignaturePost2(apiSecret string, method string, host string, path string, ts string, apiKey string, encodes string, params string) string {
	message := fmt.Sprintf("%s%s%s%s", ts, method, path, params)
	secret, _ := base64.URLEncoding.DecodeString(apiSecret)
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(message))
	return base64.URLEncoding.EncodeToString(h.Sum(nil))
}

func (b *DydxUsdcSwap) SendSignal(signals []helper.Signal) {
	for _, s := range signals {
		log.Debug("发送信号 %s", s)
		fmt.Printf("@@@@@@@@@@@@@@@Signal Type:%v\n", s.Type)
		switch s.Type {
		case helper.SignalTypeNewOrder:
			go b.takeOrder(s.Price, s.Amount, s.ClientID, s.OrderSide, s.OrderType, s.Time)
		case helper.SignalTypeCancelOrder:
			if s.OrderID != "" {
				go b.cancelOrderByID(s.OrderID)
			}
		case helper.SignalTypeCheckOrder:
			go b.checkOrderByID(s.ClientID, s.OrderID)
		case helper.SignalTypeGetPos:
			//go b.getPosition()
		case helper.SignalTypeGetEquity:
			go b.getEquity()
		}
	}
}

func (b *DydxUsdcSwap) BeforeTrade(mode helper.BeforeTradeMode) {
	b.getExchangeInfo()
	switch mode {
	case helper.BeforeTradeModePrepare:
		//b.getPosition()
	case helper.BeforeTradeModeCloseOne:
		//b.cancelAllOpenOrders(true)
		//b.cleanPosition(true)
	case helper.BeforeTradeModeCloseAll:
		//b.cancelAllOpenOrders(false)
		//b.cleanPosition(false)
	}
	//b.cancelAllOpenOrders(true)
	//b.cleanAllPositions(true)
	b.getEquity()
	b.getFees()
	b.getTicker()
}

func (b *DydxUsdcSwap) AfterTrade(mode helper.AfterTradeMode) bool {
	var isleft bool
	switch mode {
	case helper.AfterTradeModePrepare:
	case helper.AfterTradeModeCloseOne:
		b.cancelAllOpenOrders(true)
		b.cleanAllPositions(true)
	case helper.AfterTradeModeCloseAll:
		b.cancelAllOpenOrders(true)
		b.cleanAllPositions(true)
	}
	return isleft
}

// must?
func (b *DydxUsdcSwap) GetDelay() (int64, int64, int64) {
	return b.takerOrderPass.GetDelay(), b.cancelOrderPass.GetDelay(), b.systemPass.GetDelay()
}
