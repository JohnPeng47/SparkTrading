from typing import Dict
import datetime

def basic_random_walk_generator(symbol: str, prices: Dict[str, float]) -> dict:
    import random
    
    # Random walk
    prices[symbol] *= (1 + random.uniform(-0.001, 0.001))
    
    return {
        'symbol': symbol,
        'price': round(prices[symbol], 2),
        'volume': random.randint(100, 1000),
        'timestamp': datetime.now().isoformat(),
        'bid': round(prices[symbol] * 0.999, 2),
        'ask': round(prices[symbol] * 1.001, 2)
    }
