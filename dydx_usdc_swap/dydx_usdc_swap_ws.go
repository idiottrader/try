package dydx_usdc_swap

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"time"
	"zbeast/pkg/quant/broker/client/ws"
	"zbeast/pkg/quant/helper"
	"zbeast/third/decimal"
	"zbeast/third/log"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
	"go.uber.org/atomic"
)

var (
	DydxWsPubUrl       = "wss://api.dydx.exchange/v3/ws"
	DydxWsPriUrl       = "wss://api.dydx.exchange/v3/ws"
	DydxRestUrl        = "https://api.dydx.exchange"
	json               = jsoniter.ConfigCompatibleWithStandardLibrary
	wsPublicHandyPool  fastjson.ParserPool
	wsPrivateHandyPool fastjson.ParserPool
)

type DydxUsdcSwapWs struct {
	exchangeName helper.BrokerName
	params       *helper.BrokerConfig
	pubWs        *ws.WS
	priWs        *ws.WS
	cb           helper.CallbackFunc
	connectOnce  sync.Once
	stopCPub     chan struct{}
	stopCPri     chan struct{}
	needAuth     bool
	needTicker   bool
	needDepth    bool
	needTrade    bool
	needPartial  bool

	pair     helper.Pair
	symbol   string
	tradeMsg *helper.TradeMsg
	pairInfo *helper.ExchangeInfo
	pubWsUrl string
	priWsUrl string
	restUrl  string
	//logged        bool
	takerFee atomic.Float64
	makerFee atomic.Float64
}

func NewWs(params *helper.BrokerConfig, msg *helper.TradeMsg, info *helper.ExchangeInfo, cb helper.CallbackFunc) *DydxUsdcSwapWs {
	if msg == nil {
		msg = &helper.TradeMsg{}
	}
	w := &DydxUsdcSwapWs{
		exchangeName: helper.BrokernameDydxUsdcSwap,
		params:       params,
		needAuth:     params.NeedAuth,
		needTicker:   params.NeedTicker,
		needDepth:    params.NeedDepth,
		needPartial:  params.NeedPartial,
		needTrade:    params.NeedTrade,
		cb:           cb,
		//colo
		pair:     params.Pair,
		tradeMsg: msg,
		pairInfo: info,
		pubWsUrl: DydxWsPubUrl,
		priWsUrl: DydxWsPriUrl,
		restUrl:  DydxRestUrl,
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

func (w *DydxUsdcSwapWs) pong() []byte {
	return []byte("pong")
}

func generateSignature(apiSecret string, method string, host string, path string, ts string, apiKey string, encodes string) string {
	var message string

	if len(encodes) == 0 {
		message = ts + method + path
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

func (w *DydxUsdcSwapWs) SubScribePub() error {
	if w.needTicker {
		p := map[string]interface{}{
			"type":           "subscribe",
			"channel":        "v3_orderbook",
			"id":             w.symbol,
			"includeOffsets": true,
		}
		msg, err := json.Marshal(p)
		if err != nil {
			log.Errorf("[ws][%s] json encode error, %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	if w.params.NeedTrade {
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
			log.Errorf("[ws][%s] json encode error, %s", w.exchangeName.String(), err)
		}
		w.pubWs.SendMessage(msg)
	}
	return nil
}

func getWsLogin(params *helper.BrokerConfig) []byte {
	apiKey := params.AccessKey
	secretKey := params.SecretKey
	passphrase := params.PassKey
	now := time.Now().UTC()
	timestamp := now.Format(time.RFC3339)
	sign := generateSignature(secretKey, "GET", "/ws/accounts", "/ws/accounts", timestamp, apiKey, "")
	login := map[string]interface{}{
		"type":          "subscribe",
		"channel":       "v3_accounts",
		"accountNumber": "0",
		"apiKey":        apiKey,
		"passphrase":    passphrase,
		"timestamp":     timestamp,
		"signature":     sign,
	}
	msg, _ := json.Marshal(login)
	return msg
}

func (w *DydxUsdcSwapWs) SubScribePri() error {
	msg := getWsLogin(w.params)
	w.priWs.SendMessage(msg)
	return nil
}

func (w *DydxUsdcSwapWs) realSubscribe() {
	p := map[string]interface{}{
		"op": "subscribe",
		"args": []interface{}{
			map[string]interface{}{
				"channel": "account",
				"asset":   "USD",
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
		log.Errorf("[ws] [%s] json encode error, %s", w.exchangeName.String(), err)

	}
	w.priWs.SendMessage(msg)
}

func (w *DydxUsdcSwapWs) Run() {
	w.connectOnce.Do(func() {
		if w.needTicker || w.needTrade || w.needDepth || w.needPartial {
			var err1 error
			w.stopCPub, err1 = w.pubWs.Serve()
			if err1 != nil {
				w.cb.OnExit("dydx usdc swap public ws connect fail")
			}
		}
		if w.needAuth {
			var err2 error
			w.stopCPri, err2 = w.priWs.Serve()
			if err2 != nil {
				w.cb.OnExit("dydx usdc swap private ws connect fail")
			}
		}
	})
}

func (w *DydxUsdcSwapWs) Stop() {
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

func (w *DydxUsdcSwapWs) pubHandler(msg []byte, ts int64) {
	p := wsPublicHandyPool.Get()
	defer wsPublicHandyPool.Put(p)
	value, err := p.ParseBytes(msg)
	//fmt.Printf("Receive public message: %s\n", value)
	if err != nil {
		log.Errorf("dydx pub ws wrong msg:%v  err: %v", value, err)
		return
	}
	if value.Exists("contents") && string(value.GetStringBytes("type")) == "subscribed" {
		symbol := helper.BytesToString(value.GetStringBytes("id"))
		if symbol == w.symbol {
			contents := value.GetObject("contents")
			asksValue := contents.Get("asks")
			bidsValue := contents.Get("bids")
			//if asksValue == nil || bidsValue == nil {
			//	fmt.Println("contents中缺少Asks或Bids")
			//	return
			//}
			asksArray, _ := asksValue.Array()
			bidsArray, _ := bidsValue.Array()

			t := &w.tradeMsg.Depth
			t.Lock.Lock()
			if len(t.Bids) > 0 {
				t.Bids = []helper.DepthItem{}
			}
			// 如果t.Asks有值，则清空它
			if len(t.Asks) > 0 {
				t.Asks = []helper.DepthItem{}
			}
			//t.Lock.Lock()
			//defer t.Lock.Unlock()
			//var offset int64
			for _, item := range asksArray {
				price := fastfloat.ParseBestEffort(string(item.GetStringBytes("price")))
				size := fastfloat.ParseBestEffort(string(item.GetStringBytes("size")))
				if size != 0 {
					t.Asks = append(t.Asks, helper.DepthItem{
						Price:  price,
						Amount: size,
					})
				}
			}

			for _, item := range bidsArray {
				price := fastfloat.ParseBestEffort(string(item.GetStringBytes("price")))
				size := fastfloat.ParseBestEffort(string(item.GetStringBytes("size")))
				if size != 0 {
					t.Bids = append(t.Bids, helper.DepthItem{
						Price:  price,
						Amount: size,
					})
				}
			}

			t.Lock.Unlock()
		}

	} else if value.Exists("contents") && string(value.GetStringBytes("type")) == "channel_data" {
		symbol := helper.BytesToString(value.GetStringBytes("id"))
		if symbol == w.symbol {
			contents := value.Get("contents")
			bidsInfo := contents.Get("bids")
			asksInfo := contents.Get("asks")
			bidsArr, _ := bidsInfo.Array()
			asksArr, _ := asksInfo.Array()
			ticker := &w.tradeMsg.Ticker
			offset, _ := strconv.Atoi(string(contents.GetStringBytes("offset")))
			offsetOld := ticker.ID.Load()
			if int64(offset) < offsetOld {
				return
			}
			ticker.ID.Store(int64(offset))
			//fmt.Printf("###bidsArr%v\n", bidsArr)
			//fmt.Printf("###asksArr%v\n", asksArr)
			t := &w.tradeMsg.Depth
			//tA := &w.tradeMsg.Depth
			for _, bidValue := range bidsArr {
				//var itmbid2 []float64
				//fmt.Printf("1111111$$$$$NEW BIDS%v\n", bidValue)
				//t := &w.tradeMsg.Depth
				//fmt.Printf("000$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ERROR Final BID1:%v\n", t.Bids)
				t.Lock.Lock()
				//defer t.Lock.Unlock()
				bidArr := bidValue.GetArray()
				price := fastfloat.ParseBestEffort(string(bidArr[0].GetStringBytes()))
				size := fastfloat.ParseBestEffort(string(bidArr[1].GetStringBytes()))

				b := []float64{price, size}
				newItem := helper.DepthItem{
					Price:  b[0],
					Amount: b[1],
				}
				//fmt.Printf("222222$$$$$NEW BIDS%v\n", b)
				//fmt.Printf("111$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ERROR Final BID1:%v\n", t.Bids)

				for idx, itm := range t.Bids {
					if itm.Price < b[0] {
						if b[1] != 0.0 {
							// Insert b at index idx
							t.Bids = append(t.Bids[:idx], append([]helper.DepthItem{newItem}, t.Bids[idx:]...)...)
							break
						}
					} else if itm.Price == b[0] {
						if b[1] == 0.0 {
							t.Bids = append(t.Bids[:idx], t.Bids[idx+1:]...)
							//fmt.Printf("update2222$$$$$$$$$$$@@@@t.bids:%v", t.Bids)
						} else {
							t.Bids[idx] = helper.DepthItem{
								Price:  b[0],
								Amount: b[1],
							}
							//fmt.Printf("update3333$$$$$$$$$$$@@@@t.bids:%v", t.Bids)
							if idx == 0 {
								for {
									depthItemB2 := t.Asks[0]                                    // Getting the first item from t.Asks
									itmbid2 := []float64{depthItemB2.Price, depthItemB2.Amount} // Converting it to []float64
									//itmbid2 = t.Asks[0]
									if itmbid2[0] <= b[0] {
										//orderBookEntry := w.orderbook[symbol]
										t.Asks = t.Asks[1:]
										//w.orderbook[symbol] = orderBookEntry
									} else {
										break
									}
								}
							}
						}
						//t.Lock.Unlock()
						//w.cb.OnDepth(ts)
						break
					}
					// If neither condition is met, it will continue to the next loop iteration by default.
				}
				if len(t.Bids) > 0 {
					//fmt.Printf("@@@@@@@@@Final BID1:%v\n", t.Bids[0])
				} else {
					//fmt.Printf("2$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ERROR Final BID1:%v\n", t.Bids)
				}
				//fmt.Printf("@@@@@@@@@Final BID1:%v", t.Bids[0])
				t.Lock.Unlock()
				if len(t.Bids) > 0 {
					ticker.Bp.Store(t.Bids[0].Price)
					ticker.Bq.Store(t.Bids[0].Amount)
				}
			}

			for _, askValue := range asksArr {
				//fmt.Printf("11111111$$$$$NEW ASKS%v\n", askValue)
				//tA := &w.tradeMsg.Depth
				//t := &w.tradeMsg.Depth
				t.Lock.Lock()
				//defer t.Lock.Unlock()
				askArr := askValue.GetArray()
				priceA := fastfloat.ParseBestEffort(string(askArr[0].GetStringBytes()))
				sizeA := fastfloat.ParseBestEffort(string(askArr[1].GetStringBytes()))

				a := []float64{priceA, sizeA}
				newItem := helper.DepthItem{
					Price:  a[0],
					Amount: a[1],
				}
				//fmt.Printf("22222222222$$$$$NEW ASKS%v\n", a)
				//fmt.Printf("111$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ERROR Final ASK1:%v\n", t.Bids)

				for idx, itm := range t.Asks {
					if itm.Price > a[0] {
						if a[1] != 0.0 {
							// Insert b at index idx
							t.Asks = append(t.Asks[:idx], append([]helper.DepthItem{newItem}, t.Asks[idx:]...)...)
							break
						}
					} else if itm.Price == a[0] {
						//if b[2] < itm[2] {
						//	//w.orderbookLock.Unlock()
						//	break
						//}
						if a[1] == 0.0 {
							t.Asks = append(t.Asks[:idx], t.Asks[idx+1:]...)
							//fmt.Printf("update2222$$$$$$$$$$$@@@@t.bids:%v", t.Bids)
						} else {
							t.Asks[idx] = helper.DepthItem{
								Price:  a[0],
								Amount: a[1],
							}
							if idx == 0 {
								//depthItemB2 := t.Asks[0] // Assuming t.Asks[0] is of type helper.DepthItem
								//itmbid2 := []float64{depthItemB2.Price, depthItemB2.Amount}

								// Handle potential bug in DYDX
								for {
									depthItemA2 := t.Bids[0]                                    // Getting the first item from t.Asks
									itmask2 := []float64{depthItemA2.Price, depthItemA2.Amount} // Converting it to []float64
									//itmbid2 = t.Asks[0]
									if itmask2[0] >= a[0] {
										//orderBookEntry := w.orderbook[symbol]
										t.Bids = t.Bids[1:]
										//w.orderbook[symbol] = orderBookEntry
									} else {
										break
									}
								}
							}
						}
						//t.Lock.Unlock()
						//w.cb.OnDepth(ts)
						break
					}
					// If neither condition is met, it will continue to the next loop iteration by default.
				}
				if len(t.Asks) > 0 {
					//fmt.Printf("@@@@@@@@@Final ASK1:%v\n", t.Asks[0])
				} else {
					//fmt.Printf("2$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ERROR Final ASK1:%v\n", t.Asks)
				}
				//fmt.Printf("@@@@@@@@@Final BID1:%v", t.Bids[0])
				t.Lock.Unlock()
				if len(t.Asks) > 0 {
					ticker.Ap.Store(t.Asks[0].Price)
					ticker.Aq.Store(t.Asks[0].Amount)
				}
			}
			ticker.Mp.Store(ticker.Price())
			//fmt.Printf("@@@@@@@@@@@@FINAL MP:%v\n", ticker.Price())
			w.cb.OnTicker(ts)
		}
	}
}

func (w *DydxUsdcSwapWs) priHandler(msg []byte, ts int64) {
	p := wsPrivateHandyPool.Get()
	defer wsPrivateHandyPool.Put(p)
	value, err := p.ParseBytes(msg)
	fmt.Printf("Received Private value: %s\n", value)
	if err != nil {
		log.Errorf("dydx private ws wrong err:%v", err)
		return
	}
	if value.Exists("contents") {
		datas := value.Get("contents")
		if datas.Exists("account") {
			fmt.Printf("ACCOUNT!!!!!!:%s\n", datas.Get("account"))
			acc_channel := datas.Get("account")
			t := &w.tradeMsg.Equity
			t.Lock.Lock()
			t.Cash = fastfloat.ParseBestEffort(helper.BytesToString(acc_channel.GetStringBytes("equity")))
			t.CashFree = fastfloat.ParseBestEffort(helper.BytesToString(acc_channel.GetStringBytes("freeCollateral")))
			t.Lock.Unlock()
			w.cb.OnEquity(ts)
		}
		if datas.Exists("orders") {
			log.Infof("@@!!Ws order channel data %v\n", datas)
			order_channel := datas.GetArray("orders")
			if len(order_channel) != 0 {
				for _, v := range order_channel {
					instId := helper.BytesToString(v.GetStringBytes("market"))
					if instId == w.symbol {
						order := helper.OrderEvent{}
						order.OrderID = string(v.GetStringBytes("id"))
						order.ClientID = string(v.GetStringBytes("clientId"))
						state := helper.BytesToString(v.GetStringBytes("status"))
						switch state {
						case "CANCELED", "FILLED":
							order.Type = helper.OrderEventTypeREMOVE
							fmt.Printf("@@@order state:%v####oid:%v\n", state, order.OrderID)
							size, _ := decimal.NewFromString(helper.BytesToString(v.GetStringBytes("size")))
							remainsize, _ := decimal.NewFromString(helper.BytesToString(v.GetStringBytes("remainingSize")))
							filled := size.Sub(remainsize)
							order.Filled = filled
							if !order.Filled.IsZero() {
								order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(v.GetStringBytes("price")))
								//order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
							}
						case "PartiallyFilled":
							order.Type = helper.OrderEventTypeNEW
							size, _ := decimal.NewFromString(helper.BytesToString(v.GetStringBytes("size")))
							remainsize, _ := decimal.NewFromString(helper.BytesToString(v.GetStringBytes("remainingSize")))
							filled := size.Sub(remainsize)
							order.Filled = filled
							if !order.Filled.IsZero() {
								order.PartTraded = true
								//order.FilledPrice = fastfloat.ParseBestEffort(helper.BytesToString(v.GetStringBytes("price")))
								//order.CashFee = decimal.NewFromFloat(fastfloat.ParseBestEffort(helper.BytesToString(data.GetStringBytes("cumExecFee"))))
							}
						//case "OPEN":
						default:
							order.Type = helper.OrderEventTypeNEW
							fmt.Printf("!!!order status:%v\n#####oid:%v", state, order.OrderID)
							//w.cb.OnOrder(ts, order)
						}
						w.cb.OnOrder(ts, order)
					}
				}
			}
		}
	} else if value.Exists("type") {
		e := string(value.GetStringBytes("error"))
		fmt.Printf("need reconnect?%s", e)
	}

}

func (w *DydxUsdcSwapWs) GetFee() (float64, float64) {
	return w.takerFee.Load(), w.makerFee.Load()
}
