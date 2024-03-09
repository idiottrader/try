// 基于https://docs.acx.finance/api文档开发
package acxfinance_usdt_swap

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"zbeast/pkg/quant/broker/client/rest"
	"zbeast/pkg/quant/broker/client/ws"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

var (
	AcxfinanceWsPubUrl = "wss://app-api.acx.finance/arbitrum/public"
	AcxfinanceWsPriUrl = "wss://app-api.acx.finance/arbitrum/private"
	AcxfinanceRestUrl  = "https://app-api.acx.finance/arbitrum"
	json               = jsoniter.ConfigCompatibleWithStandardLibrary
	wsPublicHandyPool  fastjson.ParserPool
	wsPrivateHandyPool fastjson.ParserPool
)

//// 人工输入费率
//const (
//	acxTakerFee = 0.0003
//	acxMakerFee = 0.00015
//)

type AcxfinanceUsdtSwapWs struct {
	exchangeName helper.BrokerName    // 交易所名称
	params       *helper.BrokerConfig // 配置文件
	pubWs        *ws.WS               // 继承ws
	priWs        *ws.WS               // 继承ws
	cb           helper.CallbackFunc  // 回调函数集合
	connectOnce  sync.Once            // 连接一次
	stopCPub     chan struct{}        // 接受停机信号
	stopCPri     chan struct{}        // 接受停机信号
	needAuth     bool                 // 是否需要私有连接
	needTicker   bool                 // 是否需要bbo
	needDepth    bool                 // 是否需要深度
	needTrade    bool                 //是否需要公开成交
	needPartial  bool                 // 是否需要增量深度
	pair         helper.Pair          // 交易对
	symbol       string               // 交易对
	tradeMsg     *helper.TradeMsg     // 交易数据结构
	pairInfo     *helper.ExchangeInfo // 主交易对的交易规则
	pubWsUrl     string               // 公共ws地址
	priWsUrl     string               // 私有ws地址
	restUrl      string               // rest地址
	logged       bool                 // 是否登录
	takerFee     atomic.Float64       // taker费率
	makerFee     atomic.Float64       // maker费率
	client       *rest.Client         // 通用rest客户端
}

func NewWs(params *helper.BrokerConfig, msg *helper.TradeMsg, info *helper.ExchangeInfo, cb helper.CallbackFunc) *AcxfinanceUsdtSwapWs {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}
	w := &AcxfinanceUsdtSwapWs{
		exchangeName: helper.BrokernameAcxfinanceUsdtSwap,
		params:       params,
		needAuth:     params.NeedAuth,
		needTicker:   params.NeedTicker,
		needDepth:    params.NeedDepth,
		needPartial:  params.NeedPartial,
		needTrade:    params.NeedTrade,
		cb:           cb,
		pair:         params.Pair,
		tradeMsg:     msg,
		pairInfo:     info,
		pubWsUrl:     AcxfinanceWsPubUrl,
		priWsUrl:     AcxfinanceWsPriUrl,
		restUrl:      AcxfinanceRestUrl,
		client:       rest.NewClient(params.ProxyURL, params.LocalAddr),
	}
	w.symbol = pairToSymbol(w.pair)

	w.pubWs = ws.NewWS(w.pubWsUrl, params.LocalAddr, params.ProxyURL, w.pubHandler)
	w.pubWs.SetPingFunc(w.pong)
	w.pubWs.SetSubscribe(w.SubScribePub)

	w.priWs = ws.NewWS(w.priWsUrl, params.LocalAddr, params.ProxyURL, w.priHandler)
	w.priWs.SetPingFunc(w.pong)
	w.priWs.SetSubscribe(w.SubScribePri)

	return w
}

func generateSignature(apiKey, secretKey, timestamp, method, host, path string, params map[string]string) string {
	signatureString := method + "\n" + strings.ToLower(host) + "\n" + path + "\n" + timestamp + "\n" + apiKey

	if len(params) > 0 {
		signatureString += "\n"

		keys := make([]string, 0, len(params))
		for k := range params {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		orderedParams := make([]string, 0, len(params))
		for _, k := range keys {
			orderedParams = append(orderedParams, url.QueryEscape(k)+"="+url.QueryEscape(params[k]))
		}
		//fmt.Println("orderedParams:", orderedParams)
		signatureString += strings.Join(orderedParams, "&")
	}

	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(signatureString))

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (w *AcxfinanceUsdtSwapWs) pong() []byte {
	return []byte("pong")
}

func (w *AcxfinanceUsdtSwapWs) SubScribePub() error {
	// bbo
	if w.needTicker {
		p := map[string]interface{}{
			"op": "subscribe",
			"args": []interface{}{
				map[string]interface{}{
					"symbol":  w.symbol,
					"channel": "depth5",
				},
			},
		}

		msg, err := json.Marshal(p)
		if err != nil {
			log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	//有时候需要用trade来补充更新depth
	if w.needTrade {
		p := map[string]interface{}{
			"op": "subscribe",
			"args": []interface{}{
				map[string]interface{}{
					"symbol":  w.symbol,
					"channel": "trade",
				},
			},
		}
		msg, err := json.Marshal(p)
		if err != nil {
			log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	return nil
}

func getWsLogin(params *helper.BrokerConfig) []byte {
	// auth ws
	apiKey := params.AccessKey
	secretKey := params.SecretKey
	timestamp := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	sign := generateSignature(apiKey, secretKey, timestamp, "GET", "app-api.acx.finance", "/users/self/verify", nil)

	login := map[string]interface{}{
		"op": "auth",
		"args": []interface{}{
			map[string]interface{}{
				"apiKey":    params.AccessKey,
				"timestamp": timestamp,
				"signature": sign,
			},
		},
	}
	msg, _ := json.Marshal(login)
	return msg
}

func (w *AcxfinanceUsdtSwapWs) SubScribePri() error {
	// login first
	msg := getWsLogin(w.params)
	w.priWs.SendMessage(msg)
	// 实际登录注册信息被放在后面了, 在 realSubscribe 里面
	// todo 这里是不是做了2次 realSubscribe 的尝试？是为了冗余保护吗
	go func() {
		time.Sleep(3 * time.Second)
		if !w.logged {
			w.realSubscribe()
		}
	}()
	return nil
}

func (w *AcxfinanceUsdtSwapWs) realSubscribe() {
	p := map[string]interface{}{
		"op": "subscribe",
		"args": []interface{}{
			map[string]interface{}{
				"channel": "account",
				"asset":   "USDC",
			},
			map[string]interface{}{
				"channel": "position",
				"symbol":  "",
			},
			map[string]interface{}{
				"channel": "order",
				"symbol":  w.symbol,
			},
		},
	}
	msg, err := json.Marshal(p)
	if err != nil {
		log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
	}
	w.priWs.SendMessage(msg)
}

func (w *AcxfinanceUsdtSwapWs) Run() {
	w.connectOnce.Do(func() {
		w.getFees()
		log.Infof("[ws] maker手续费 %v taker手续费 %v", w.makerFee.Load(), w.takerFee.Load())
		if w.needTicker || w.needTrade || w.needDepth || w.needPartial {
			var err1 error
			w.stopCPub, err1 = w.pubWs.Serve()
			if err1 != nil {
				w.cb.OnExit("okx usdt swap public ws 连接失败")
			}
		}
		if w.needAuth {
			var err2 error
			w.stopCPri, err2 = w.priWs.Serve()
			if err2 != nil {
				w.cb.OnExit("okx usdt swap private ws 连接失败")
			}
		}
	})
}

func (w *AcxfinanceUsdtSwapWs) Stop() {
	if w.needTicker || w.needTrade || w.needDepth || w.needPartial {
		if w.stopCPub != nil {
			w.stopCPub <- struct{}{}
		}
	}
	if w.needAuth {
		if w.stopCPri != nil {
			w.stopCPri <- struct{}{}
		}
	}
}

// 处理公有ws的处理器
func (w *AcxfinanceUsdtSwapWs) pubHandler(msg []byte, ts int64) {
	//解析
	p := wsPublicHandyPool.Get()
	defer wsPublicHandyPool.Put(p)
	value, err := p.ParseBytes(msg)
	if !(len(msg) < 10 && helper.BytesToString(msg) == "ping") {
		if err != nil {
			log.Errorf("acxfinance usdt swap ws解析msg出错 msg: %v err: %v", value, err)
			return
		}
	}
	//// log
	if value.Exists("data") {
		// 存在data的基本都是行情推送，优先级最高，立刻处理
		symbol := helper.BytesToString(value.GetStringBytes("symbol"))
		if symbol == w.symbol {
			op := helper.BytesToString(value.GetStringBytes("channel"))
			switch op {
			case "depth5":
				t := &w.tradeMsg.Ticker
				dataStr := helper.BytesToString(value.GetStringBytes("data"))
				dataValue, err := p.Parse(dataStr)
				if err != nil {
					log.Errorf("解析 data 字段出错: %v", err)
					return
				}
				asks := dataValue.GetArray("asks")
				bids := dataValue.GetArray("bids")
				ask := asks[0]
				ap := ask.Get("0").String()
				ap = strings.Trim(ap, "\"")
				aq := ask.Get("1").String()
				aq = strings.Trim(aq, "\"")

				bid := bids[0]
				bp := bid.Get("0").String()
				bp = strings.Trim(bp, "\"")
				bq := bid.Get("1").String()
				bq = strings.Trim(bq, "\"")

				t.Ap.Store(fastfloat.ParseBestEffort(ap))
				t.Aq.Store(fastfloat.ParseBestEffort(aq))
				t.Bp.Store(fastfloat.ParseBestEffort(bp))
				t.Bq.Store(fastfloat.ParseBestEffort(bq))
				t.Mp.Store(t.Price())
				w.cb.OnTicker(ts)
			case "trade":
				t := &w.tradeMsg.Trade
				dataValue := value.Get("data")
				if err != nil {
					log.Errorf("解析 data 字段出错: %v", err)
					return
				}
				side := helper.BytesToString(dataValue.GetStringBytes("side"))
				price := fastfloat.ParseBestEffort(helper.BytesToString(dataValue.GetStringBytes("price")))
				amount := fastfloat.ParseBestEffort(helper.BytesToString(dataValue.GetStringBytes("qty")))
				switch side {
				case "BUY":
					t.Update(helper.TradeSideBuy, amount, price)
				case "SELL":
					t.Update(helper.TradeSideSell, amount, price)
				}
				w.cb.OnTrade(ts)
			}
		}
	} else if value.Exists("event") { // event常见于事件汇报，一般都可以忽略，只需要看看是否有error
		e := helper.BytesToString(value.GetStringBytes("event"))
		if e == "error" {
			log.Errorf("[pub ws][%s] error , %v", w.exchangeName.String(), value)
		}
	}
}

// 处理私有ws的处理器
func (w *AcxfinanceUsdtSwapWs) priHandler(msg []byte, ts int64) {
	//fmt.Printf("Received Private message: %s\n", string(msg))
	// 解析
	p := wsPrivateHandyPool.Get()
	defer wsPrivateHandyPool.Put(p)
	value, err := p.ParseBytes(msg)
	if !(len(msg) < 10 && helper.BytesToString(msg) == "ping") {
		if err != nil {
			log.Errorf("okx usdt swap ws解析msg出错 err:%v", err)
			return
		}
	}
	if value.Exists("data") {
		op := helper.BytesToString(value.GetStringBytes("channel"))
		switch op {
		case "account":
			datas := value.Get("data")
			t := &w.tradeMsg.Equity
			t.Lock.Lock()
			t.Cash = fastfloat.ParseBestEffort(helper.BytesToString(datas.GetStringBytes("totalMarginBalance"))) //totalWalletBalance
			t.CashFree = fastfloat.ParseBestEffort(helper.BytesToString(datas.GetStringBytes("totalAvailableBalance")))
			t.Lock.Unlock()
			w.cb.OnEquity(ts)
		case "position":
			datas := value.GetArray("data")
			log.Debugf("收到 ws pos 推送 %s", helper.BytesToString(msg))
			t := &w.tradeMsg.Position
			t.Reset()
			for _, data := range datas {
				instId := helper.BytesToString(data.GetStringBytes("symbol"))
				if instId == w.symbol {
					t.Lock.Lock()
					ps := helper.BytesToString(data.GetStringBytes("positionSide"))
					px := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("openPrice")))
					sz := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("positionAmt")))
					switch ps {
					case "long":
						t.LongAvg = px
						t.LongPos = decimal.NewFromFloat(sz)
					case "short":
						t.ShortAvg = px
						t.ShortPos = decimal.NewFromFloat(sz)
					case "NET":
						if sz > 0 {
							t.LongAvg = px
							t.LongPos = decimal.NewFromFloat(sz)
						} else {
							t.ShortAvg = px
							t.ShortPos = decimal.NewFromFloat(-sz)
						}
					}
					t.Lock.Unlock()
				}
			}
			w.cb.OnPosition(ts)
		case "order":
			datas := value.Get("data")
			//fmt.Printf("Received Order message: %s\n", string(msg))
			log.Debugf("收到 ws order 推送 %s", helper.BytesToString(msg))
			instId := helper.BytesToString(value.GetStringBytes("symbol"))
			if instId == w.symbol {
				order := helper.OrderEvent{}
				order.OrderID = string(datas.GetStringBytes("orderId"))
				order.ClientID = string(datas.GetStringBytes("clientOrderId"))
				state := helper.BytesToString(datas.GetStringBytes("status")) //NEW-->FILLED
				switch state {
				case "CANCELED", "FILLED":
					order.Type = helper.OrderEventTypeREMOVE
					szStr, _ := decimal.NewFromString(helper.BytesToString(datas.GetStringBytes("executedQty")))
					if szStr.GreaterThan(decimal.Zero) {
						order.Filled = szStr
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(datas.GetStringBytes("avgPrice")))
						dealValue, _ := decimal.NewFromString(helper.BytesToString(datas.GetStringBytes("cumQuote")))
						ordermaker := datas.GetBool("maker")
						if ordermaker {
							acxMakerFeeDecimal := decimal.NewFromFloat(w.makerFee.Load())
							cashFee := dealValue.Mul(acxMakerFeeDecimal)
							order.CashFee = cashFee
							//scaled := cashFee.Mul(decimal.NewFromInt(100)) // Scale value by 100
							//rounded := scaled.Ceil()                       // Round up to nearest whole number
							//order.CashFee = rounded.Div(decimal.NewFromInt(100))
						} else {
							acxTakerFeeDecimal := decimal.NewFromFloat(w.takerFee.Load())
							cashFee := dealValue.Mul(acxTakerFeeDecimal)
							order.CashFee = cashFee
							//scaled := cashFee.Mul(decimal.NewFromInt(100)) // Scale value by 100
							//rounded := scaled.Ceil()                       // Round up to nearest whole number
							//order.CashFee = rounded.Div(decimal.NewFromInt(100))
						}
					}
				case "NEW":
					order.Type = helper.OrderEventTypeNEW
				case "PARTIALLY_FILLED":
					order.Type = helper.OrderEventTypeNEW
					order.PartTraded = true
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				w.cb.OnOrder(ts, order)
			}
		}
	} else if value.Exists("event") { // event常见于事件汇报，一般都可以忽略，只需要看看是否有error
		e := helper.BytesToString(value.GetStringBytes("event"))
		switch e {
		case "error":
			log.Errorf("[pri ws][%s] error , %v", w.exchangeName.String(), value)
		case "login":
			// 登录订阅放在这里
			w.logged = true
			// todo 这里是不是做了2次 realSubscribe 的尝试？是为了冗余保护吗
			w.realSubscribe()
		}
	}
}

// call 专用于acxfinance_usdt_swap的发起http请求函数
func (w *AcxfinanceUsdtSwapWs) call(reqMethod string, reqUrl string, params map[string]interface{}, needSign bool, respHandler rest.FastHttpRespHandler) error {
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
			sign = createSignatureGet(w.params.SecretKey, reqMethod, specialurl, reqUrl, strconv.FormatInt(now, 10), w.params.AccessKey, params)
		case http.MethodPost:
			sign = createSignaturePost(w.params.SecretKey, reqMethod, specialurl, reqUrl, strconv.FormatInt(now, 10), w.params.AccessKey)
		}
		requestHeaders["ACX-SIGNATURE"] = sign
		requestHeaders["ACX-API-KEY"] = w.params.AccessKey
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

	status, err := w.client.Request(reqMethod, s.String(), requestBodyBytes, requestHeaders, respHandler)

	if err != nil {
		//fmt.Printf("Call Func Err %v", err)
		return err
	}
	if status != 200 {
		return fmt.Errorf("status:%d", status)
	}
	return nil
}

func (w *AcxfinanceUsdtSwapWs) getFees() {
	url := "/api/v1/account/commissionRate"
	p := handyPool.Get()
	defer handyPool.Put(p)
	var handlerErr error
	var err error
	var value *fastjson.Value

	err = w.call(http.MethodGet, url, nil, true, func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
		value, handlerErr = p.ParseBytes(respBody)
		fmt.Printf("Received value: %s\n", value)
		if handlerErr != nil {
			return
		}
		data := value.Get("data")
		makerCommissionRate := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("makerCommissionRate")))
		takerCommissionRate := fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("takerCommissionRate")))
		w.makerFee.Store(makerCommissionRate)
		w.takerFee.Store(takerCommissionRate)
	})
	if err != nil {
		//得检查是否有限频提示
		log.Warnf("get fees err %v", err)
	}
}

// GetFee 获取费率
func (w *AcxfinanceUsdtSwapWs) GetFee() (float64, float64) {
	return w.takerFee.Load(), w.makerFee.Load()
}
