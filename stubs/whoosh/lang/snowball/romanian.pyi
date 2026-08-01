from .bases import _StandardStemmer
from whoosh.compat import u as u

class RomanianStemmer(_StandardStemmer):
    def stem(self, word): ...
