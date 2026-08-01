from .bases import _StandardStemmer
from whoosh.compat import u as u

class DutchStemmer(_StandardStemmer):
    def stem(self, word): ...
