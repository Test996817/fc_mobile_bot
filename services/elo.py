class EloCalculator:
    K_FACTOR = 32
    
    def calculate(self, rating_a: int, rating_b: int, score_a: float) -> Tuple[int, int, int]:
        expected_a = 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
        expected_b = 1 - expected_a
        
        actual_b = 1 - score_a
        
        new_rating_a = rating_a + int(self.K_FACTOR * (score_a - expected_a))
        new_rating_b = rating_b + int(self.K_FACTOR * (actual_b - expected_b))
        
        change = new_rating_a - rating_a
        return new_rating_a, new_rating_b, change
