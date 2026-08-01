from whoosh.automata.fsa import ANY as ANY, EPSILON as EPSILON, NFA as NFA, unull as unull
from whoosh.compat import unichr as unichr, xrange as xrange

def levenshtein_automaton(term, k, prefix: int = 0): ...
