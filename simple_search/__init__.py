import os

pdir = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
os.environ["NLTK_DATA"] = os.path.join(pdir, "nltk_data")
