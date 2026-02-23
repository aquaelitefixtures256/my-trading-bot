//+------------------------------------------------------------------+
//|                                                    Venom37.mq5   |
//|  Simple EMA crossover + ATR risk + (optional) trailing & alerts  |
//+------------------------------------------------------------------+
#property strict
#include <Trade/Trade.mqh>
CTrade trade;

input double   RiskPercent      = 1.0;      // % equity risk per trade
input int      FastMAPeriod     = 20;
input int      SlowMAPeriod     = 50;
input int      ATRPeriod        = 14;
input double   SL_ATR_Mult      = 2.0;
input double   TP_ATR_Mult      = 4.0;
input bool     UseTrailingATR   = true;
input double   Trail_ATR_Mult   = 1.0;
input ENUM_TIMEFRAMES SignalTF  = PERIOD_M15;
input int      MaxSpreadPoints  = 0;        // 0 = ignore
input long     Magic            = 370037;   // unique EA ID
input bool     SendPush         = true;     // requires MetaQuotes ID set in desktop MT5

int fastH = INVALID_HANDLE, slowH = INVALID_HANDLE, atrH = INVALID_HANDLE;
datetime lastBar=0;

int OnInit()
{
   trade.SetExpertMagicNumber(Magic);

   fastH = iMA(_Symbol, SignalTF, FastMAPeriod, 0, MODE_EMA, PRICE_CLOSE);
   slowH = iMA(_Symbol, SignalTF, SlowMAPeriod, 0, MODE_EMA, PRICE_CLOSE);
   atrH  = iATR(_Symbol, SignalTF, ATRPeriod);

   if(fastH==INVALID_HANDLE || slowH==INVALID_HANDLE || atrH==INVALID_HANDLE)
   {
      Print("Venom37: indicator handle error");
      return(INIT_FAILED);
   }
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   if(fastH!=INVALID_HANDLE) IndicatorRelease(fastH);
   if(slowH!=INVALID_HANDLE) IndicatorRelease(slowH);
   if(atrH !=INVALID_HANDLE) IndicatorRelease(atrH);
}

bool IsNewBar()
{
   datetime t = iTime(_Symbol, SignalTF, 0);
   if(t!=0 && t!=lastBar){ lastBar=t; return true; }
   return false;
}

bool CopyMA(double &fast0,double &fast1,double &slow0,double &slow1)
{
   double fb[2], sb[2];
   if(CopyBuffer(fastH,0,0,2,fb)!=2) return false;
   if(CopyBuffer(slowH,0,0,2,sb)!=2) return false;
   fast0 = fb[0]; fast1 = fb[1];
   slow0 = sb[0]; slow1 = sb[1];
   return true;
}

bool GetATRPoints(double &atrPts)
{
   double ab[2];
   if(CopyBuffer(atrH,0,0,2,ab)!=2) return false;
   double atr = ab[1]; // use closed bar
   if(atr<=0) return false;
   atrPts = atr/_Point;
   return true;
}

double LotsForRisk(double stopPts)
{
   if(stopPts<=0) return 0.0;
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double riskAmt = equity*(RiskPercent/100.0);

   double tickValue=0.0, tickSize=0.0;
   SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE, tickValue);
   SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE,  tickSize);

   if(tickSize<=0 || tickValue<=0) return 0.0;

   // monetary value per 1 point for 1 lot:
   double valuePerPoint = tickValue * (_Point / tickSize);

   double lots = riskAmt / (stopPts * valuePerPoint);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);

   if(step<=0) step=0.01;
   lots = MathFloor(lots/step)*step;
   lots = MathMax(minL, MathMin(maxL, lots));
   return lots;
}

void Notify(string msg)
{
   if(SendPush) SendNotification("Venom37: "+msg);
   Print("Venom37: ", msg);
}

bool SpreadOK()
{
   if(MaxSpreadPoints<=0) return true;
   double bid=SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask=SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   if(!bid || !ask) return false;
   double spreadPts=(ask-bid)/_Point;
   return (spreadPts <= MaxSpreadPoints);
}

void CloseOpposite(int desiredType)
{
   // One position per symbol (netting). If existing opposite, close it.
   if(PositionSelect(_Symbol))
   {
      long type = PositionGetInteger(POSITION_TYPE);
      long magic= PositionGetInteger(POSITION_MAGIC);
      if(magic==Magic && type!=desiredType) // opposite
      {
         if(trade.PositionClose(_Symbol))
            Notify("Closed opposite position on signal flip");
      }
   }
}

void ManageTrailing()
{
   if(!UseTrailingATR) return;
   if(!PositionSelect(_Symbol)) return;
   long magic = PositionGetInteger(POSITION_MAGIC);
   if(magic!=Magic) return;

   double atrPts=0; if(!GetATRPoints(atrPts)) return;
   double trailPts = Trail_ATR_Mult*atrPts;
   if(trailPts<=0) return;

   long   type = PositionGetInteger(POSITION_TYPE);
   double sl   = PositionGetDouble(POSITION_SL);
   double tp   = PositionGetDouble(POSITION_TP);
   double bid  = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask  = SymbolInfoDouble(_Symbol, SYMBOL_ASK);

   if(type==POSITION_TYPE_BUY)
   {
      double newSL = bid - trailPts*_Point;
      if((sl==0 || newSL>sl) && newSL<bid)
         trade.PositionModify(_Symbol,newSL,tp);
   }
   else if(type==POSITION_TYPE_SELL)
   {
      double newSL = ask + trailPts*_Point;
      if((sl==0 || newSL<sl) && newSL>ask)
         trade.PositionModify(_Symbol,newSL,tp);
   }
}

void CheckSignalsAndTrade()
{
   if(!SpreadOK()) return;

   double f0,f1,s0,s1; if(!CopyMA(f0,f1,s0,s1)) return;
   double atrPts=0;     if(!GetATRPoints(atrPts)) return;

   double slPts = SL_ATR_Mult*atrPts;
   double tpPts = TP_ATR_Mult*atrPts;
   if(slPts<=0 || tpPts<=0) return;

   // Bullish cross: fast crosses above slow
   if(f1<s1 && f0>s0)
   {
      CloseOpposite(POSITION_TYPE_BUY);
      if(!PositionSelect(_Symbol) || PositionGetInteger(POSITION_MAGIC)!=Magic)
      {
         double lots = LotsForRisk(slPts);
         if(lots>0)
         {
            double ask=SymbolInfoDouble(_Symbol, SYMBOL_ASK);
            double sl = ask - slPts*_Point;
            double tp = ask + tpPts*_Point;
            if(trade.Buy(lots,NULL,0,sl,tp))
               Notify("BUY opened "+DoubleToString(lots,2));
         }
      }
   }
   // Bearish cross: fast crosses below slow
   if(f1>s1 && f0<s0)
   {
      CloseOpposite(POSITION_TYPE_SELL);
      if(!PositionSelect(_Symbol) || PositionGetInteger(POSITION_MAGIC)!=Magic)
      {
         double bid=SymbolInfoDouble(_Symbol, SYMBOL_BID);
         double sl = bid + slPts*_Point;
         double tp = bid - tpPts*_Point;
         double lots = LotsForRisk(slPts);
         if(lots>0)
         {
            if(trade.Sell(lots,NULL,0,sl,tp))
               Notify("SELL opened "+DoubleToString(lots,2));
         }
      }
   }
}

void OnTick()
{
   if(IsNewBar()) CheckSignalsAndTrade();
   ManageTrailing();
}
