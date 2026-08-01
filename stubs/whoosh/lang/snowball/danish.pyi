from .bases import _ScandinavianStemmer
from whoosh.compat import u as u

class DanishStemmer(_ScandinavianStemmer):
    def stem(self, word): ...
