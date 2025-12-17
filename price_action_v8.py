"""
Price Action Analyzer v8.2 - GEMINI RECOMMENDATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 "Price First, OI Second" - Prevent False Signals
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRITICAL FEATURES:
1. Breakout Detection (20-period high/low)
2. EMA 9 & 21 alignment
3. VWAP position check
4. Swing High/Low (Resistance/Support)
5. 3-Signal Rule implementation

WHY THIS MATTERS:
- v8.1 generates signals in sideways markets
- This module catches choppy conditions
- Only allows trades with price confirmation

INTEGRATION:
Add to signal_engine.py BEFORE OI checks!
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class PriceActionAnalyzer:
    """
    Analyze price action to confirm OI signals
    Implementation of Gemini's "Price First, OI Second" principle
    """
    
    def __init__(self):
        self.price_history = []  # Store price snapshots
        self.candle_history = []  # Store 5-min candles
        self.vwap = None
        self.ema_9 = None
        self.ema_21 = None
        
        # Tracking variables
        self.last_breakout_time = None
        self.last_resistance = None
        self.last_support = None
    
    def store_price_snapshot(self, price: float, volume: float = 0, timestamp: datetime = None):
        """
        Store price snapshot for analysis
        Call this every scan (every 60 seconds)
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        snapshot = {
            'timestamp': timestamp,
            'price': price,
            'volume': volume
        }
        
        self.price_history.append(snapshot)
        
        # Keep only last 2 hours (120 snapshots)
        if len(self.price_history) > 120:
            self.price_history = self.price_history[-120:]
        
        # Update indicators if enough data
        if len(self.price_history) >= 9:
            self._update_indicators()
    
    def store_candle(self, candle_data: Dict):
        """
        Store 5-minute candle data
        Expected format: {'open': x, 'high': x, 'low': x, 'close': x, 'volume': x, 'timestamp': x}
        """
        self.candle_history.append(candle_data)
        
        # Keep only last 50 candles (4 hours)
        if len(self.candle_history) > 50:
            self.candle_history = self.candle_history[-50:]
    
    def _update_indicators(self):
        """Calculate VWAP and EMAs"""
        if len(self.price_history) < 9:
            return
        
        df = pd.DataFrame(self.price_history)
        
        # VWAP calculation
        if df['volume'].sum() > 0:
            df['cumulative_pv'] = (df['price'] * df['volume']).cumsum()
            df['cumulative_v'] = df['volume'].cumsum()
            df['vwap'] = df['cumulative_pv'] / df['cumulative_v']
            self.vwap = df['vwap'].iloc[-1]
        
        # EMA 9
        if len(df) >= 9:
            self.ema_9 = df['price'].ewm(span=9, adjust=False).mean().iloc[-1]
        
        # EMA 21
        if len(df) >= 21:
            self.ema_21 = df['price'].ewm(span=21, adjust=False).mean().iloc[-1]
    
    def check_breakout(self, current_price: float) -> Dict:
        """
        🔥 CRITICAL: Check if price has broken out
        
        Returns:
            {'type': 'BULLISH'/'BEARISH'/'NONE', 'strength': 0-10, 'reason': str}
        """
        if len(self.price_history) < 20:
            return {
                'type': 'NONE',
                'strength': 0,
                'reason': 'Insufficient data (need 20 periods)',
                'confirmed': False
            }
        
        df = pd.DataFrame(self.price_history)
        
        # Get 20-period high/low
        recent_high = df['price'].tail(20).max()
        recent_low = df['price'].tail(20).min()
        
        # Calculate range
        price_range = recent_high - recent_low
        breakout_threshold = price_range * 0.001  # 0.1% buffer
        
        result = {
            'type': 'NONE',
            'strength': 0,
            'reason': '',
            'confirmed': False,
            'resistance': recent_high,
            'support': recent_low
        }
        
        # 🟢 Bullish Breakout
        if current_price > (recent_high + breakout_threshold):
            result['type'] = 'BULLISH'
            result['strength'] = 9
            result['reason'] = f'Bullish breakout above {recent_high:.2f}'
            result['confirmed'] = True
            
            self.last_breakout_time = datetime.now()
            self.last_resistance = recent_high
            
            logger.info(f"🟢 BULLISH BREAKOUT: {current_price:.2f} > {recent_high:.2f}")
        
        # 🔴 Bearish Breakdown
        elif current_price < (recent_low - breakout_threshold):
            result['type'] = 'BEARISH'
            result['strength'] = 9
            result['reason'] = f'Bearish breakdown below {recent_low:.2f}'
            result['confirmed'] = True
            
            self.last_breakout_time = datetime.now()
            self.last_support = recent_low
            
            logger.info(f"🔴 BEARISH BREAKDOWN: {current_price:.2f} < {recent_low:.2f}")
        
        # 📊 Near breakout
        elif current_price > (recent_high * 0.998):  # Within 0.2% of high
            result['type'] = 'BULLISH'
            result['strength'] = 5
            result['reason'] = f'Approaching resistance at {recent_high:.2f}'
            result['confirmed'] = False
        
        elif current_price < (recent_low * 1.002):  # Within 0.2% of low
            result['type'] = 'BEARISH'
            result['strength'] = 5
            result['reason'] = f'Approaching support at {recent_low:.2f}'
            result['confirmed'] = False
        
        return result
    
    def check_ema_alignment(self, current_price: float) -> Dict:
        """
        Check EMA 9 and EMA 21 alignment
        Gemini: "Price > VWAP > EMA9 > EMA21 = Strong Bull"
        """
        if not self.ema_9:
            return {
                'aligned': False,
                'strength': 'UNKNOWN',
                'score': 0,
                'details': ['EMA data not ready']
            }
        
        result = {
            'aligned': False,
            'bullish': False,
            'bearish': False,
            'strength': 'WEAK',
            'score': 0,
            'details': []
        }
        
        # 🟢 Bullish Alignment
        if current_price > self.ema_9:
            result['bullish'] = True
            result['score'] += 10
            result['details'].append(f'Price > EMA9 ({self.ema_9:.2f})')
            
            if self.ema_21 and self.ema_9 > self.ema_21:
                result['score'] += 10
                result['details'].append(f'EMA9 > EMA21 (Golden cross)')
                result['strength'] = 'STRONG'
                result['aligned'] = True
            else:
                result['strength'] = 'MEDIUM'
        
        # 🔴 Bearish Alignment
        elif current_price < self.ema_9:
            result['bearish'] = True
            result['score'] -= 10
            result['details'].append(f'Price < EMA9 ({self.ema_9:.2f})')
            
            if self.ema_21 and self.ema_9 < self.ema_21:
                result['score'] -= 10
                result['details'].append(f'EMA9 < EMA21 (Death cross)')
                result['strength'] = 'STRONG'
                result['aligned'] = True
            else:
                result['strength'] = 'MEDIUM'
        
        return result
    
    def check_vwap_position(self, current_price: float) -> Dict:
        """
        Check VWAP position
        Gemini: "कधीही VWAP च्या खाली CE खरेदी करू नका!"
        """
        if not self.vwap:
            return {
                'valid': False,
                'score': 0,
                'reason': 'VWAP not calculated yet'
            }
        
        distance_pct = ((current_price - self.vwap) / self.vwap) * 100
        
        result = {
            'valid': True,
            'above': current_price > self.vwap,
            'below': current_price < self.vwap,
            'distance_pct': distance_pct,
            'score': 0,
            'reason': ''
        }
        
        # 🟢 Above VWAP (Good for CE_BUY)
        if current_price > self.vwap:
            if distance_pct > 0.5:  # More than 0.5% above
                result['score'] = 15
                result['reason'] = f'Strong above VWAP (+{distance_pct:.2f}%)'
            elif distance_pct > 0.2:
                result['score'] = 10
                result['reason'] = f'Above VWAP (+{distance_pct:.2f}%)'
            else:
                result['score'] = 5
                result['reason'] = f'Just above VWAP (+{distance_pct:.2f}%)'
        
        # 🔴 Below VWAP (Good for PE_BUY)
        else:
            if distance_pct < -0.5:  # More than 0.5% below
                result['score'] = 15
                result['reason'] = f'Strong below VWAP ({distance_pct:.2f}%)'
            elif distance_pct < -0.2:
                result['score'] = 10
                result['reason'] = f'Below VWAP ({distance_pct:.2f}%)'
            else:
                result['score'] = 5
                result['reason'] = f'Just below VWAP ({distance_pct:.2f}%)'
        
        return result
    
    def check_resistance_support(self, current_price: float) -> Dict:
        """
        🚨 CRITICAL: Check if price near resistance/support
        Gemini: "Resistance ला अडकलेली असेल तर OI सिग्नल ignore कर!"
        """
        if len(self.price_history) < 50:
            return {
                'near_resistance': False,
                'near_support': False,
                'penalty': 0
            }
        
        df = pd.DataFrame(self.price_history)
        
        # Find swing highs (last 50 periods)
        recent_highs = df['price'].tail(50).nlargest(5)
        avg_resistance = recent_highs.mean()
        
        # Find swing lows
        recent_lows = df['price'].tail(50).nsmallest(5)
        avg_support = recent_lows.mean()
        
        # Check proximity (within 0.3%)
        resistance_threshold = avg_resistance * 0.997  # 0.3% below resistance
        support_threshold = avg_support * 1.003  # 0.3% above support
        
        result = {
            'near_resistance': current_price >= resistance_threshold,
            'near_support': current_price <= support_threshold,
            'resistance_level': avg_resistance,
            'support_level': avg_support,
            'penalty': 0,
            'warnings': []
        }
        
        # 🚨 Apply penalties
        if result['near_resistance']:
            result['penalty'] = -20  # Big penalty for buying at resistance!
            result['warnings'].append(f'⚠️ Near resistance at {avg_resistance:.2f}')
            logger.warning(f"⚠️ Price near resistance: {avg_resistance:.2f}")
        
        if result['near_support']:
            result['penalty'] = -20  # Big penalty for selling at support!
            result['warnings'].append(f'⚠️ Near support at {avg_support:.2f}')
            logger.warning(f"⚠️ Price near support: {avg_support:.2f}")
        
        return result
    
    def comprehensive_check(self, current_price: float, signal_type: str = None) -> Dict:
        """
        🎯 MASTER METHOD: Complete price action analysis
        
        Gemini's 3-Signal Rule:
        1. Breakout confirmed? ✅
        2. Above/below VWAP + EMAs? ✅
        3. Not at resistance/support? ✅
        
        Args:
            current_price: Current price
            signal_type: 'CE_BUY' or 'PE_BUY' (if known)
        
        Returns:
            Comprehensive analysis with verdict
        """
        # Get all checks
        breakout = self.check_breakout(current_price)
        ema_align = self.check_ema_alignment(current_price)
        vwap_pos = self.check_vwap_position(current_price)
        levels = self.check_resistance_support(current_price)
        
        # Initialize scores
        bullish_score = 0
        bearish_score = 0
        reasons = []
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1. BREAKOUT (40 points - Most Important!)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if breakout['confirmed']:
            if breakout['type'] == 'BULLISH':
                bullish_score += 40
                reasons.append(f"🟢 {breakout['reason']}")
            elif breakout['type'] == 'BEARISH':
                bearish_score += 40
                reasons.append(f"🔴 {breakout['reason']}")
        elif breakout['strength'] >= 5:
            # Near breakout but not confirmed
            if breakout['type'] == 'BULLISH':
                bullish_score += 15
                reasons.append(f"📊 {breakout['reason']}")
            elif breakout['type'] == 'BEARISH':
                bearish_score += 15
                reasons.append(f"📊 {breakout['reason']}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 2. VWAP POSITION (25 points)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if vwap_pos['valid']:
            if vwap_pos['above']:
                bullish_score += vwap_pos['score']
                reasons.append(f"✅ {vwap_pos['reason']}")
            else:
                bearish_score += vwap_pos['score']
                reasons.append(f"✅ {vwap_pos['reason']}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3. EMA ALIGNMENT (20 points)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if ema_align['aligned']:
            if ema_align['bullish']:
                bullish_score += 20
                reasons.extend(ema_align['details'])
            elif ema_align['bearish']:
                bearish_score += 20
                reasons.extend(ema_align['details'])
        elif abs(ema_align['score']) > 0:
            if ema_align['score'] > 0:
                bullish_score += 10
            else:
                bearish_score += 10
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 4. RESISTANCE/SUPPORT PENALTY (-20 points)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if levels['near_resistance']:
            bullish_score += levels['penalty']  # Penalty for buying at resistance
            reasons.extend(levels['warnings'])
        
        if levels['near_support']:
            bearish_score += levels['penalty']  # Penalty for selling at support
            reasons.extend(levels['warnings'])
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # FINAL VERDICT
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        result = {
            'bullish': bullish_score >= 50,  # Need at least 50/85 points
            'bearish': bearish_score >= 50,
            'bullish_score': bullish_score,
            'bearish_score': bearish_score,
            'confidence': max(bullish_score, bearish_score),
            'reasons': reasons,
            'verdict': 'NONE',
            'breakout_confirmed': breakout['confirmed'],
            'details': {
                'breakout': breakout,
                'ema_alignment': ema_align,
                'vwap_position': vwap_pos,
                'levels': levels
            }
        }
        
        # Set verdict
        if result['bullish'] and result['confidence'] >= 50:
            result['verdict'] = 'BULLISH_CONFIRMED'
        elif result['bearish'] and result['confidence'] >= 50:
            result['verdict'] = 'BEARISH_CONFIRMED'
        else:
            result['verdict'] = 'NO_CLEAR_SETUP'
        
        # Additional check for signal type matching
        if signal_type:
            if signal_type == 'CE_BUY' and not result['bullish']:
                result['verdict'] = 'PRICE_ACTION_REJECTED_CE'
            elif signal_type == 'PE_BUY' and not result['bearish']:
                result['verdict'] = 'PRICE_ACTION_REJECTED_PE'
        
        logger.info(f"📊 Price Action Verdict: {result['verdict']}")
        logger.info(f"   Scores → Bull: {bullish_score} | Bear: {bearish_score}")
        
        return result


# ============================================
# USAGE EXAMPLE
# ============================================

"""
# In main.py:

from price_action_v8 import PriceActionAnalyzer

class NiftyTradingBot:
    def __init__(self):
        # ... existing code ...
        self.price_analyzer = PriceActionAnalyzer()
    
    async def scan_market(self):
        # ... fetch data ...
        
        # Store price for analysis
        self.price_analyzer.store_price_snapshot(
            price=futures_ltp,
            volume=futures_volume,
            timestamp=now_ist
        )
        
        # ... OI analysis ...
        
        # BEFORE generating signal, check price action!
        price_check = self.price_analyzer.comprehensive_check(futures_ltp)
        
        if price_check['verdict'] == 'NO_CLEAR_SETUP':
            logger.info("⏸️ Price Action: No clear setup - Skipping")
            return
        
        # Now check OI signals...
        signal = self.signal_gen.generate(...)
        
        # Final validation
        if signal:
            if signal.signal_type == SignalType.CE_BUY and not price_check['bullish']:
                logger.warning("⚠️ CE_BUY rejected by price action")
                return
            
            if signal.signal_type == SignalType.PE_BUY and not price_check['bearish']:
                logger.warning("⚠️ PE_BUY rejected by price action")
                return
        
        # Signal passed all checks! ✅
"""
