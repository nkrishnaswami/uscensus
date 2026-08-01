from .bases import _ScandinavianStemmer
from whoosh.compat import u as u

class SwedishStemmer(_ScandinavianStemmer):
    def stem(self, word): ...
