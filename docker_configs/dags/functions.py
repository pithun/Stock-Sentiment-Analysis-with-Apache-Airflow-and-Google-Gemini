from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag

def lemmatized(sentence):
     # Lemmatizer function using parts of speech tagging.
    lemmatizer = WordNetLemmatizer()
    wnl = WordNetLemmatizer()
    lemmatized = []
    for word, tag in pos_tag(sentence.split()):
        if tag.startswith("NN"):
            lemmatized.append(wnl.lemmatize(word, pos='n'))
        elif tag.startswith('VB'):
            lemmatized.append(wnl.lemmatize(word, pos='v'))
        elif tag.startswith('JJ'):
            lemmatized.append(wnl.lemmatize(word, pos='a'))
        else:
            lemmatized.append(word)
    return lemmatized