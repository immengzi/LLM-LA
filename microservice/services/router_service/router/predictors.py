# -*- coding: utf-8 -*-
"""
Very simple length predictor: use character length as proxy for tokens.
You can later swap this for a proper model.
"""

from typing import Optional


class SimpleLengthPredictor:
    name = "char-len"

    def predict_out_tokens(self, prompt: str, req_id: str) -> Optional[int]:
        # "medium" guess: input length / 2
        return max(1, len(prompt) // 2)


_predictor = SimpleLengthPredictor()


def get_length_predictor() -> SimpleLengthPredictor:
    return _predictor
