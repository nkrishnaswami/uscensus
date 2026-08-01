from .bases import _StandardStemmer
from whoosh.compat import u as u

class EnglishStemmer(_StandardStemmer):
    def stem(self, word): ...
