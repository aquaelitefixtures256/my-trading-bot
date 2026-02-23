//+------------------------------------------------------------------+
//|                                                  Venom37.mq5     |
//|    Advanced EA: EMA crossover + ATR sizing + flat-market filter  |
//|    Features: ATR filter, EMA slope filter, trailing, spread min  |
//|    Author: Venom37 (you)                                          |
//+------------------------------------------------------------------+
#property copyright "Venom37"
#property version   "1.10"
#property strict

#include <Trade/Trade.mqh>
CTrade  trade;

// ---------- INPUTS ----------
input double   RiskPercent        = 1.0;        // % equity risk per trade
input int      FastMAPeriod       = 20;         // fast EMA period
input int      SlowMAPeriod       = 50;         // slow EMA period
input int      ATRPeriod          = 14;         // ATR period
input double   SL_ATR_Mult        = 2.0;        // SL = ATR * this
input double   TP_ATR_Mult        = 4.0;        // TP = ATR * this
input bool     UseTrailingATR     = true;       // enable trailing
input double   Trail_ATR_Mult     = 1.0;        // trailing distance (ATR multiplier)
input ENUM_TIMEFRAMES SignalTF    = PERIOD_M15; // timeframe used for signals (chart should match)
input int      MaxSpreadPoints    = 0;          // 0 = disabled, otherwise max spread in points
input long     Magic              = 370037;     // magic number
input bool     SendPush           = true;       // send push notifications
input double   MinATRPoints       = 6.0;        // minimum ATR (in points) to consider market not flat
input double   MinEMASlopePoints  = 2.0;        // minimum absolute EMA slope (in points) to consider trending
input bool     RespectStopLevel   = true;       // check SYMBOL_TRADE_STOPS_LEVEL before placing stops
input bool     UseTradingHours    = false;      // don't trade outside trading window if true
input int      TradeStartHour     = 7;          // server hour (0-23)
input int      TradeEndHour       = 22;         // server hour (0-23)
input int      MaxDailyTrades     = 10;         // 0 = unlimited

// ---------- GLOBALS ----------
int    hFastMA = INVALID_HANDLE;
int    hSlowMA = INVALID_HANDLE;
int    hATR    = INVALID_HANDLE;
datetime lastSignalBar = 0;
int    tradesToday = 0;
datetime lastResetDay = 0;

// ---------- UTIL ----------
// returns server hour (0-23)
int ServerHour()
{
   datetime t = TimeCurrent();
   return(TimeHour(t));
}

// check trading hours window
bool InTradingHours()
{
   if(!UseTradingHours) return true;
   int h = ServerHour();
   if(TradeStartHour <= TradeEndHour) return (h >= TradeStartHour && h < TradeEndHour);
   // wrap-around case
   return (h >= TradeStartHour || h < TradeEndHour);
}

// reset daily trade counter (once per day)
void ResetDailyCounterIfNeeded()
{
   datetime now = TimeCurrent();
   MqlDateTime dt; TimeToStruct(now, dt);
   if(dt.day_of_year != lastResetDay)
   {
      tradesToday = 0;
      lastResetDay = dt.day_of_year;
   }
}

// send notification (safe)
void NotifyEA(string msg)
{
   string out = "Venom37: " + msg;
   Print(out);
   if(SendPush)
   {
      // Try/catch is not available so guard by checking SendPush
      SendNotification(out);
   }
}

// ---------- INIT / DEINIT ----------
int OnInit()
{
   // create indicator handles on the desired timeframe (SignalTF)
   hFastMA = iMA(_Symbol, SignalTF, FastMAPeriod, 0, MODE_EMA, PRICE_CLOSE);
   hSlowMA = iMA(_Symbol, SignalTF, SlowMAPeriod, 0, MODE_EMA, PRICE_CLOSE);
   hATR    = iATR(_Symbol, SignalTF, ATRPeriod);

   if(hFastMA == INVALID_HANDLE || hSlowMA == INVALID_HANDLE || hATR == INVALID_HANDLE)
   {
      PrintFormat("Venom37: failed to create indicator handles (%d %d %d)", hFastMA, hSlowMA, hATR);
      return(INIT_FAILED);
   }

   trade.SetExpertMagicNumber(Magic);
   trade.SetDeviationInPoints(10); // default slippage tolerance

   // initialize counters
   ResetDailyCounterIfNeeded();

   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   if(hFastMA != INVALID_HANDLE) IndicatorRelease(hFastMA);
   if(hSlowMA != INVALID_HANDLE) IndicatorRelease(hSlowMA);
   if(hATR    != INVALID_HANDLE) IndicatorRelease(hATR);
}

// ---------- HELPERS ----------
bool GetLatestEMA(double &fastNow, double &fastPrev, double &slowNow, double &slowPrev)
{
   double fa[2], sa[2];
   if(CopyBuffer(hFastMA, 0, 0, 2, fa) != 2) return false;
   if(CopyBuffer(hSlowMA, 0, 0, 2, sa) != 2) return false;
   fastNow  = fa[0]; fastPrev = fa[1];
   slowNow  = sa[0]; slowPrev = sa[1];
   return true;
}

bool GetATRPoints(double &atrPts)
{
   double ab[2];
   if(CopyBuffer(hATR, 0, 0, 2, ab) != 2) return false;
   double atrValue = ab[1]; // use closed (previous) bar ATR for stability
   if(atrValue <= 0.0) return false;
   atrPts = atrValue / _Point;
   return true;
}

// compute lot size given stop distance in points; returns 0 if cannot trade
double LotsForRisk(double stopPts)
{
   if(stopPts <= 0.0) return 0.0;
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double riskMoney = equity * (RiskPercent / 100.0);

   double tickValue=0.0, tickSize=0.0;
   SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE, tickValue);
   SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE,  tickSize);
   double contract = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_CONTRACT_SIZE);

   if(tickSize <= 0.0 || tickValue <= 0.0 || contract <= 0.0) return 0.0;

   // value per 1 point for 1 lot (approx)
   double valuePerPoint = (tickValue / tickSize);

   double lots = riskMoney / (stopPts * valuePerPoint);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);

   if(step <= 0.0) step = 0.01;
   // normalize lots to allowed step
   double rounded = MathFloor(lots / step) * step;
   rounded = MathMax(minL, MathMin(maxL, rounded));
   return rounded;
}

// check spread in points
bool SpreadOK()
{
   if(MaxSpreadPoints <= 0) return true;
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   if(bid <= 0.0 || ask <= 0.0) return false;
   double spreadPts = (ask - bid) / _Point;
   return (spreadPts <= MaxSpreadPoints);
}

// ensure SL/TP distance respects broker stop-level
bool AdjustStopsIfNeeded(double &sl, double &tp, double price, bool isBuy)
{
   if(!RespectStopLevel) return true;

   long stop_level = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL); // in points
   if(stop_level < 0) stop_level = 0;
   double minDist = MathMax((double)stop_level, 1.0); // at least 1 point

   double distSL = MathAbs(price - sl) / _Point;
   double distTP = MathAbs(tp - price) / _Point;

   // if either is too close, push them further out proportionally
   bool changed = false;
   if(distSL < minDist)
   {
      if(isBuy) sl = price - minDist * _Point;
      else      sl = price + minDist * _Point;
      changed = true;
   }
   if(distTP < minDist)
   {
      if(isBuy) tp = price + minDist * _Point;
      else      tp = price - minDist * _Point;
      changed = true;
   }
   return true;
}

// close opposite position opened by same magic
void CloseOppositeIfExist(int desiredType)
{
   // iterate through open positions
   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      if(PositionGetTicket(i) <= 0) continue;
      if(!PositionSelectByTicket(PositionGetTicket(i))) continue;
      string sym = PositionGetString(POSITION_SYMBOL);
      long   magic = PositionGetInteger(POSITION_MAGIC);
      long   type  = PositionGetInteger(POSITION_TYPE);
      if(sym == _Symbol && magic == Magic && type != desiredType)
      {
         ulong ticket = PositionGetTicket(i);
         trade.PositionClose(ticket);
         NotifyEA("Closed opposite pos ticket " + (string)ticket);
      }
   }
}

// trailing management
void ManageTrailing()
{
   if(!UseTrailingATR) return;
   // iterate positions to find ours
   for(int i = 0; i < PositionsTotal(); ++i)
   {
      if(PositionGetTicket(i) <= 0) continue;
      if(!PositionSelectByTicket(PositionGetTicket(i))) continue;
      string sym = PositionGetString(POSITION_SYMBOL);
      long   magic = PositionGetInteger(POSITION_MAGIC);
      if(sym != _Symbol || magic != Magic) continue;

      long type = PositionGetInteger(POSITION_TYPE);
      double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);
      double sl = PositionGetDouble(POSITION_SL);
      double tp = PositionGetDouble(POSITION_TP);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);

      double atrPts = 0.0;
      if(!GetATRPoints(atrPts)) continue;
      double trailPts = Trail_ATR_Mult * atrPts;
      if(trailPts <= 0.0) continue;

      if(type == POSITION_TYPE_BUY)
      {
         double newSL = bid - trailPts * _Point;
         if((sl == 0.0 || newSL > sl) && newSL < bid)
            trade.PositionModify(PositionGetTicket(i), newSL, tp);
      }
      else if(type == POSITION_TYPE_SELL)
      {
         double newSL = ask + trailPts * _Point;
         if((sl == 0.0 || newSL < sl) && newSL > ask)
            trade.PositionModify(PositionGetTicket(i), newSL, tp);
      }
   }
}

// main signal check and trade execution
void CheckSignalsAndTrade()
{
   // daily counter reset
   ResetDailyCounterIfNeeded();

   if(MaxDailyTrades > 0 && tradesToday >= MaxDailyTrades)
   {
      // reached today's quota
      return;
   }

   if(!InTradingHours()) return;
   if(!SpreadOK()) return;

   double fastNow, fastPrev, slowNow, slowPrev;
   if(!GetLatestEMA(fastNow, fastPrev, slowNow, slowPrev)) return;

   double atrPts = 0.0;
   if(!GetATRPoints(atrPts)) return;

   // Flat market filters
   if(atrPts < MinATRPoints)
   {
      // volatility too low
      //Print("Venom37: ATR below threshold -> skipping");
      return;
   }

   double slopeFast = MathAbs(fastNow - fastPrev) / _Point; // points
   double slopeSlow = MathAbs(slowNow - slowPrev) / _Point; // points
   if(slopeFast < MinEMASlopePoints && slopeSlow < MinEMASlopePoints)
   {
      // EMA slopes too flat
      return;
   }

   double slPts = SL_ATR_Mult * atrPts;
   double tpPts = TP_ATR_Mult * atrPts;
   if(slPts <= 0.0 || tpPts <= 0.0) return;

   // bullish cross: previous fast < previous slow AND now fast > now slow
   bool bullish = (fastPrev < slowPrev && fastNow > slowNow);
   // bearish cross:
   bool bearish = (fastPrev > slowPrev && fastNow < slowNow);

   if(!bullish && !bearish) return;

   // Ensure we do not duplicate with existing positions for same magic+symbol
   bool havePositionForSymbol = false;
   for(int i = 0; i < PositionsTotal(); ++i)
   {
      if(PositionGetTicket(i) <= 0) continue;
      if(!PositionSelectByTicket(PositionGetTicket(i))) continue;
      string sym = PositionGetString(POSITION_SYMBOL);
      long   magic = PositionGetInteger(POSITION_MAGIC);
      if(sym == _Symbol && magic == Magic) { havePositionForSymbol = true; break; }
   }
   if(havePositionForSymbol) return;

   // compute lots
   double lots = LotsForRisk(slPts);
   if(lots <= 0.0) return;

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   if(ask <= 0.0 || bid <= 0.0) return;

   if(bullish)
   {
      double price = ask;
      double sl = price - slPts * _Point;
      double tp = price + tpPts * _Point;
      AdjustStopsIfNeeded(sl, tp, price, true);
      CloseOppositeIfExist(POSITION_TYPE_BUY);

      bool ok = trade.Buy(lots, _Symbol, price, sl, tp);
      if(ok)
      {
         tradesToday++;
         NotifyEA("BUY opened " + DoubleToString(lots, 2));
      }
   }
   else if(bearish)
   {
      double price = bid;
      double sl = price + slPts * _Point;
      double tp = price - tpPts * _Point;
      AdjustStopsIfNeeded(sl, tp, price, false);
      CloseOppositeIfExist(POSITION_TYPE_SELL);

      bool ok = trade.Sell(lots, _Symbol, price, sl, tp);
      if(ok)
      {
         tradesToday++;
         NotifyEA("SELL opened " + DoubleToString(lots, 2));
      }
   }
}

// ---------- MAIN ----------
void OnTick()
{
   // run signal logic only on new bar of SignalTF for determinism
   datetime t = iTime(_Symbol, SignalTF, 0);
   if(t == 0) return;
   if(t == lastSignalBar)
   {
      // manage trailing even intrabar
      ManageTrailing();
      return;
   }
   lastSignalBar = t;

   // primary operations
   CheckSignalsAndTrade();

   // manage trailing stops
   ManageTrailing();
}
