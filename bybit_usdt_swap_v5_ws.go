package bybit_usdt_swap_v5

import (
	"fmt"
	"strconv"
	"sync"
	"time"
	"zbeast/pkg/quant/broker/client/ws"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

type BybitUsdtSwapWs struct {
	exchangeName helper.BrokerName // 交易所名称
	params       *helper.BrokerConfig
	pubWs        *ws.WS // 继承ws
	priWs        *ws.WS // 继承ws
	cb           helper.CallbackFunc
	connectOnce  sync.Once            // 连接一次
	id           atomic.Int64         // 所有发送的消息都要唯一id
	stopCPub     chan struct{}        // 接受停机信号
	stopCPri     chan struct{}        // 接受停机信号
	needAuth     bool                 // 是否需要私有连接
	needTicker   bool                 // 是否需要bbo
	needDepth    bool                 // 是否需要深度
	needPartial  bool                 // 是否需要增量深度
	needTrade    bool                 //是否需要公开成交
	colo         bool                 // 是否colo
	pair         helper.Pair          // 目标交易品种 内部格式
	symbol       string               // 目标交易品种 交易所格式
	tradeMsg     *helper.TradeMsg     // 交易数据集合
	pairInfo     *helper.ExchangeInfo // 主交易对的交易规则
	takerFee     atomic.Float64       // taker费率
	makerFee     atomic.Float64       // maker费率
	client       *bybitUsdtSwapClient
	logged       bool // 是否登录
	tickerTopic  string
	depthTopic   string
	tradeTopic   string
}

func (w *BybitUsdtSwapWs) getExchangeInfo() {
	p := handyPool.Get()
	defer handyPool.Put(p)
	_, err := w.client.get(
		"/v5/market/instruments-info",
		map[string]interface{}{
			"category": "linear",
			"symbol":   w.symbol,
		},
		false,
		func(respBody []byte, respHeader *fasthttp.ResponseHeader) {
			value, handlerErr := p.ParseBytes(respBody)
			if handlerErr != nil {
				w.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", w.exchangeName, handlerErr.Error()))
				return
			}
			code := value.GetInt64("retCode")
			if code != 0 {
				w.cb.OnExit(fmt.Sprintf("[%s]获取交易信息错误 需要停机. %v", w.exchangeName, value))
				return
			}
			datas := value.Get("result").GetArray("list")
			for _, data := range datas {
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol == w.symbol {
					status := helper.BytesToString(data.GetStringBytes("status"))
					enabled := false
					if status == "Trading" {
						enabled = true
					}
					w.pairInfo.Pair = w.pair
					w.pairInfo.Status = enabled
					w.pairInfo.TickSize = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.Get("priceFilter").GetStringBytes("tickSize"))))
					w.pairInfo.StepSize = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("qtyStep"))))
					w.pairInfo.MinOrderAmount, _ = decimal.NewFromString(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("minOrderQty")))
					w.pairInfo.MaxOrderAmount, _ = decimal.NewFromString(helper.BytesToString(data.Get("lotSizeFilter").GetStringBytes("maxOrderQty")))
					w.pairInfo.Multi = decimal.NewFromFloat(1)
					w.pairInfo.MaxOrderValue = decimal.NewFromFloat(20000)
					w.pairInfo.MinOrderValue = decimal.NewFromFloat(10)
				}
			}
		},
	)
	if err != nil {
		w.cb.OnExit(fmt.Sprintf("[%s]获取交易信息失败 需要停机. %s", w.exchangeName, err.Error()))
	}
}

func (w *BybitUsdtSwapWs) getReqId() string {
	w.id.Add(1)
	return strconv.FormatInt(w.id.Load(), 10)
}

func (w *BybitUsdtSwapWs) ping() []byte {
	m, _ := json.Marshal(map[string]interface{}{
		"op":     "ping",
		"req_id": w.getReqId(),
	})
	return m
}

func (w *BybitUsdtSwapWs) getWsLogin() []byte {
	// login first
	ts := fmt.Sprintf("%d", GetAdjustTime().UnixMilli()+100000) // expire time
	sign, _ := HmacSHA256Sign(w.params.SecretKey, "GET/realtime"+ts)
	login := map[string]interface{}{
		"req_id": w.getReqId(),
		"op":     "auth",
		"args":   []interface{}{w.params.AccessKey, ts, sign},
	}
	msg, _ := json.Marshal(login)
	return msg
}

// GetFee 获取费率
func (w *BybitUsdtSwapWs) GetFee() (float64, float64) {
	return w.takerFee.Load(), w.makerFee.Load()
}

func (w *BybitUsdtSwapWs) pubHandler(msg []byte, ts int64) {
	// 解析
	p := wsPubHandyPool.Get()
	defer wsPubHandyPool.Put(p)
	//fmt.Println(string(msg))
	value, err := p.ParseBytes(msg)
	if !(len(msg) < 10 && helper.BytesToString(msg) == "pong") {
		if err != nil {
			log.Errorf("bybit usdt swap ws解析msg出错 msg: %v err: %v", value, err)
			return
		}
	}
	if value.Exists("data") {
		data := value.Get("data")
		topic := helper.BytesToString(value.GetStringBytes("topic"))
		switch topic {
		case w.tickerTopic:
			// 这里要注意，有可能ask和bid没有行情
			symbol := helper.BytesToString(data.GetStringBytes("s"))
			if symbol == w.symbol {
				id := data.GetInt64("ts")
				t := &w.tradeMsg.Ticker
				t.ID.Store(id) // 乱序 不用用这个过滤
				asks := data.GetArray("a")
				if len(asks) > 0 {
					// 注意这里有可能有0的，这代表这一档行情被删除
					// 对一档行情来说，遍历即可，较为方便，depth则需要设计算法维护和校验
					for _, askarray := range asks {
						ask, _ := askarray.Array()
						ap, _ := ask[0].StringBytes()
						aq, _ := ask[1].StringBytes()
						if helper.BytesToString(aq) != "0" {
							t.Ap.Store(fastfloat.ParseBestEffort(helper.BytesToString(ap)))
							t.Aq.Store(fastfloat.ParseBestEffort(helper.BytesToString(aq)))
						}
					}
				}
				bids := data.GetArray("b")
				if len(bids) > 0 {
					for _, bidarray := range bids {
						bid, _ := bidarray.Array()
						bp, _ := bid[0].StringBytes()
						bq, _ := bid[1].StringBytes()
						if helper.BytesToString(bq) != "0" {
							t.Bp.Store(fastfloat.ParseBestEffort(helper.BytesToString(bp)))
							t.Bq.Store(fastfloat.ParseBestEffort(helper.BytesToString(bq)))
						}
					}
				}
				t.Mp.Store(t.Price())
				if t.Ap.Load() < t.Bp.Load() && t.Ap.Load() > 0 && t.Bp.Load() > 0 {
					// 乱序推送 可能有此类异常
					//fmt.Println("bbo价格异常")
					//w.cb.OnExit(fmt.Sprintf("ticker update err. ap<bp %v", *t))
				} else {
					w.cb.OnTicker(ts)
				}
			}
		case w.tradeTopic:
			for _, trade := range data.GetArray("data") {
				symbol := helper.BytesToString(trade.GetStringBytes("s"))
				if symbol != w.symbol {
					continue
				}
				t := &w.tradeMsg.Trade
				side := helper.BytesToString(trade.GetStringBytes("S"))
				price := fastfloat.ParseBestEffort(helper.BytesToString(trade.GetStringBytes("p")))
				amount := fastfloat.ParseBestEffort(helper.BytesToString(trade.GetStringBytes("v")))
				switch side {
				case "Buy":
					t.Update(helper.TradeSideBuy, amount, price)
				case "Sell":
					t.Update(helper.TradeSideSell, amount, price)
				}
				w.cb.OnTrade(ts)
			}
		}
	}
}

func (w *BybitUsdtSwapWs) SubscribePub() error {
	// bbo
	if w.needTicker {
		w.tickerTopic = fmt.Sprintf("orderbook.1.%s", w.symbol)
		p := map[string]interface{}{
			"op":     "subscribe",
			"args":   []interface{}{w.tickerTopic},
			"req_id": w.getReqId(),
		}
		msg, err := json.Marshal(p)
		if err != nil {
			log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	//
	if w.needDepth {
		// 当前没有实现，因为只有增量的算法
		// 但是bybit比较坑，它的增量推送是没有校验码的，没法知道自己记录的还对不对
		w.depthTopic = fmt.Sprintf("orderbook.50.%s", w.symbol)
	}
	//
	if w.needPartial {
		// 未实现
		w.depthTopic = fmt.Sprintf("orderbook.50.%s", w.symbol)
	}
	if w.needTrade {
		w.tradeTopic = fmt.Sprintf("publicTrade.%s", w.symbol)
		p := map[string]interface{}{
			"op":     "subscribe",
			"args":   []interface{}{w.tradeTopic},
			"req_id": w.getReqId(),
		}
		msg, err := json.Marshal(p)
		if err != nil {
			log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	return nil
}

func (w *BybitUsdtSwapWs) priHandler(msg []byte, ts int64) {
	p := wsPriHandyPool.Get()
	defer wsPriHandyPool.Put(p)
	//fmt.Println(string(msg))
	value, err := p.ParseBytes(msg)
	if !(len(msg) < 10 && helper.BytesToString(msg) == "pong") {
		if err != nil {
			log.Errorf("bybit usdt swap ws解析msg出错 msg: %v err: %v", value, err)
			return
		}
	}
	if value.Exists("data") {
		topic := helper.BytesToString(value.GetStringBytes("topic"))
		switch topic {
		case "order":
			log.Debugf("收到 ws order 推送 %s", helper.BytesToString(msg))
			datas := value.GetArray("data")
			for _, data := range datas {
				category := helper.BytesToString(data.GetStringBytes("category"))
				if category != "linear" {
					continue
				}
				symbol := helper.BytesToString(data.GetStringBytes("symbol"))
				if symbol != w.symbol {
					continue
				}
				order := helper.OrderEvent{}
				order.OrderID = string(data.GetStringBytes("orderId"))
				order.ClientID = string(data.GetStringBytes("orderLinkId"))
				status := helper.BytesToString(data.GetStringBytes("orderStatus"))
				switch status {
				case "Filled", "Cancelled":
					order.Type = helper.OrderEventTypeREMOVE
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					// 处理一下order.filled，按照stepsize处理
					//order.Filled = fixed.NewF(math.Floor(order.Filled.Float()/w.pairInfo.StepSize+0.5) * w.pairInfo.StepSize)
					if !order.Filled.IsZero() {
						order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("avgPrice")))
						order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
					}
				case "PartiallyFilled":
					order.Type = helper.OrderEventTypeNEW
					order.Filled = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecQty"))))
					if !order.Filled.IsZero() {
						order.PartTraded = true
					}
				default:
					order.Type = helper.OrderEventTypeNEW
				}
				w.cb.OnOrder(ts, order)
			}
		}
	} else {
		op := helper.BytesToString(value.GetStringBytes("op"))
		switch op {
		case "auth":
			// 登录成功再订阅一次
			if value.GetBool("success") {
				w.logged = true
				w.realSubscribePri()
			}
		}
	}
}

func (w *BybitUsdtSwapWs) SubscribePri() error {
	w.priWs.SendMessage(w.getWsLogin())
	go func() {
		time.Sleep(3 * time.Second)
		if !w.logged {
			w.realSubscribePri()
		}
	}()
	return nil
}

func (w *BybitUsdtSwapWs) realSubscribePri() {
	p := map[string]interface{}{
		"op": "subscribe",
		"args": []interface{}{
			"order",
		},
		"req_id": w.getReqId(),
	}
	msg, err := json.Marshal(p)
	if err != nil {
		log.Errorf("[ws][%s] json encode error , %s", w.exchangeName.String(), err)
	}
	w.priWs.SendMessage(msg)

}

func (w *BybitUsdtSwapWs) Run() {
	w.getExchangeInfo()
	w.connectOnce.Do(func() {
		if w.needTicker || w.needTrade || w.needDepth || w.needPartial {
			var err1 error
			w.stopCPub, err1 = w.pubWs.Serve()
			if err1 != nil {
				w.cb.OnExit("bybit usdt swap public ws 连接失败")
			}
		}
		if w.needAuth {
			var err2 error
			w.stopCPri, err2 = w.priWs.Serve()
			if err2 != nil {
				w.cb.OnExit("bybit usdt swap private ws 连接失败")
			}
		}
	})
}

func (w *BybitUsdtSwapWs) Stop() {
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

func NewWs(params *helper.BrokerConfig, msg *helper.TradeMsg, info *helper.ExchangeInfo, cb helper.CallbackFunc) *BybitUsdtSwapWs {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}
	coloFlag := checkColo(params)
	w := &BybitUsdtSwapWs{
		exchangeName: helper.BrokernameBybitUsdtSwap,
		params:       params,
		cb:           cb,
		needAuth:     params.NeedAuth,
		needTicker:   params.NeedTicker,
		needDepth:    params.NeedDepth,
		needPartial:  params.NeedPartial,
		needTrade:    params.NeedTrade,
		colo:         coloFlag,
		pair:         params.Pair,
		tradeMsg:     msg,
		pairInfo:     info,
		symbol:       pairToSymbol(params.Pair),
		client:       NewClient(params),
	}

	w.pubWs = ws.NewWS(bybitWsPubUrl, params.LocalAddr, params.ProxyURL, w.pubHandler)
	w.pubWs.SetPingFunc(w.ping)
	w.pubWs.SetSubscribe(w.SubscribePub)

	w.priWs = ws.NewWS(bybitWsPriUrl, params.LocalAddr, params.ProxyURL, w.priHandler)
	w.priWs.SetPingFunc(w.ping)
	w.priWs.SetSubscribe(w.SubscribePri)

	return w
}
