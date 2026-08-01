from .bases import _StandardStemmer
from whoosh.compat import u as u

class PortugueseStemmer(_StandardStemmer):
    def stem(self, word): ...
