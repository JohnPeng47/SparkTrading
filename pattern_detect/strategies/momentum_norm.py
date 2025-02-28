import time
from typing import Dict, List
from pattern_detect.detect import KafkaInterface

class SimpleMovingAverageProcessor(KafkaInterface):
    def __init__(self, 
                 window_size: int = 10, 
                 momentum_threshold: float = 0.02, 
                 *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.window_size = window_size
        self.momentum_threshold = momentum_threshold
        self.price_windows: Dict[str, List[float]] = {}
    
    def process_message(self, message: Dict) -> Dict:
        symbol = message["symbol"]
        price = message["price"]
        
        if symbol not in self.price_windows:
            self.price_windows[symbol] = []
        
        window = self.price_windows[symbol]
        window.append(price)
        
        if len(window) > self.window_size:
            window.pop(0)
        
        sma = sum(window) / len(window)
        
        return {
            "symbol": symbol,
            "timestamp": message["timestamp"],
            "price": price,
            "sma": round(sma, 2)
        }
