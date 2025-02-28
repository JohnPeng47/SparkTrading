from typing import Dict
import time
import datetime

def momentum_pattern_generator(
    symbol: str, 
    prices: Dict[str, float],
    trend_strength: float = 0.002,
    trend_duration: int = 100
) -> dict:
    import random
    from math import sin, pi
    
    # Time-based trend component
    t = int(time.time())
    trend = sin(2 * pi * (t % trend_duration) / trend_duration)
    
    # Apply trend with some noise
    prices[symbol] *= (1 + trend_strength * trend + random.uniform(-0.0005, 0.0005))
    
    return {
        'symbol': symbol,
        'price': round(prices[symbol], 2),
        'volume': random.randint(100, 1000),
        'timestamp': datetime.datetime.now().isoformat()
    }
