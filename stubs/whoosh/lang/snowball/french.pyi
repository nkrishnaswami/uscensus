from .bases import _StandardStemmer
from whoosh.compat import u as u

class FrenchStemmer(_StandardStemmer):
    def stem(self, word): ...
