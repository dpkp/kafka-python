import os
import random
import string

def random_string(l):
    s = "".join(random.choice(string.letters) for i in xrange(l))
    return s

def skip_integration():
    return os.environ.get('SKIP_INTEGRATION')
