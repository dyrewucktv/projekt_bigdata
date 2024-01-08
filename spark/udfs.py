import re

from html.parser import HTMLParser
from textblob import TextBlob
from pyspark.sql.functions import *
from pyspark.sql.types import *

clean_html_text = udf(lambda text: re.sub(r"\<[^\>]+\>", "", HTMLParser().unescape(text)), StringType())
sentiment = udf(lambda text: TextBlob(text).sentiment.polarity, DoubleType())