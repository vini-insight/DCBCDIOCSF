import sys
import time
import json
import pprint
from twython import Twython
from tweepy import OAuthHandler
from tweepy import API
import glob
import pandas as pd
import numpy as np
from tweepy.streaming import StreamListener
from tweepy import Stream
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
from operator import itemgetter
from pymongo import MongoClient
# import tweepy

client = MongoClient()
db = client.secrets
collection = db.secrets
secret = None
# acessa MongoDB e retorna chaves de seguranaça da API do twitter
secret = collection.find_one({"where": "twitter developer API"})
auth = OAuthHandler(secret["consumer_key"], secret["consumer_secret"])
auth.set_access_token(secret["access_token"], secret["access_token_secret"])
# api = API(auth)
api = API(auth, wait_on_rate_limit = True)

db = client.data_challenge
# recupera lista de trends a partir de um WOEID definido
# woeid = 23424768 # BR
# woeid = 23424977 # US
woeid = 455827 # SP - BR

api_trends = api.trends_place(id = woeid) # recupera trends list
collection = db.trends_history

mydict = {}
mylisttrends = []
keywords_to_track = []
trends =  []
as_of = None
created_at = None
locations = []
id_col = None

for value in api_trends:   
    trends = value['trends']        
    as_of = value['as_of']
    created_at = value['created_at']
    locations = value['locations']

trends_now = {"trends": trends, "as_of": as_of, "created_at": created_at, "locations": locations}
id_col = collection.insert_one(trends_now).inserted_id
pprint.pprint(id_col)
pprint.pprint(db.list_collection_names())
for value in api_trends:
    for trend in value['trends']:        
        tweet_volume = trend['tweet_volume']        
        # filtra volumes nulos
        if(tweet_volume != None):
            tweet_name = trend['name']
            fxs = tweet_name.find("#")            
            tweet_name = tweet_name.replace("#", "")            
            mydict = {"name": tweet_name, "tweet_volume": tweet_volume}
            mylisttrends.append(mydict)        
        else:
            mydict = {"name": trend['name'], "tweet_volume": tweet_volume}

sl = sorted(mylisttrends, key=itemgetter('tweet_volume'), reverse=True)
# sl = sorted(mylisttrends, key=itemgetter('tweet_volume'))

# exibe lista de trends em ordem decrescente (que não tenham volume nulo)
for i in range(len(sl)):  
  print(str(i) + " " + str(sl[i]))

print("ESCOLHA uma TREND:")
indice = input()
print("ESCOLHA outra TREND:")
proximo = input()

auxdict = sl[int(indice)]
keywords_to_track.append(auxdict['name'])
auxdict = sl[int(proximo)]
keywords_to_track.append(auxdict['name'])

# for i in range(len(sl)):
# #   print(sl[i])
#   auxdict = sl[i]
#   keywords_to_track.append(auxdict['name'])
#   print(auxdict)
#   if(i == 1):
#       break

print('K E Y        W O R D S')
print(keywords_to_track)

class SListener(StreamListener):
    def __init__(self, api = None, fprefix = 'streamer'):
        self.api = api or API()
        self.counter = 0
        self.fprefix = fprefix
        self.output  = open('tweets.json', 'w')

    def on_data(self, data):
        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print("WARNING: %s" % warning['message'])
            return

    def on_status(self, status):
        self.output.write(status)
        self.counter += 1
        if self.counter >= 20000:
            self.output.close()
            self.output  = open('%s_%s.json' % (self.fprefix, time.strftime('%Y%m%d-%H%M%S')), 'w')
            self.counter = 0
        return

    def on_delete(self, status_id, user_id):
        print("aviso de exclusao")
        return

    def on_limit(self, track):
        print("Aviso de limitação. tweets perdidos: %d" % track)
        return

    def on_error(self, status_code):
        print('código de status de erro:', status_code)
        return

    def on_timeout(self):
        print("Tempo limite, dormindo por 60 segundos ...")
        time.sleep(60)
        return

listen = SListener(api) # cria uma instancia do objeto SListener
stream = Stream(auth, listen) # cria uma instancia do objeto Stream

#  debug para ver se a thread do stream.filter está rodando
if(stream.running):
  print(stream.running)
  print("RODANDO")
else:
  print(stream.running)
  print("PARADO")

stream.filter(track = keywords_to_track, is_async=True) # começa a pegar tweets aqui usando uma thread assíncrona
# TEMPO QUE VAI FICAR COLETANDO TWEETS
runtime = 300 
print("esperando....")
time.sleep(runtime)

#  debug para ver se a thread do stream.filter está rodando
if(stream.running):
  print(stream.running)
  print("RODANDO")
else:
  print(stream.running)
  print("PARADO")
print("terminou coleta de dados")

while(stream.running):
  stream.running = False

#  debug para ver se a thread do stream.filter está rodando
if(stream.running):
  print(stream.running)
  print("RODANDO")
else:
  print(stream.running)
  print("PARADO")

tweets = []
data_json  = list(glob.iglob('tweets.json'))

# analisa cada tweet e remova as linhas vazias.
# Armazene o nome da tela do usuário em 'user-screen_name'.
# Verifique se este é um tweet com mais de 140 caracteres.
# Armazene o texto estendido do tweet em 'extended_tweet-full_text'.
# Armazene o nome de tela do usuário retuitado em 'retweeted_status-user-screen_name'.
# Armazene o texto retuitado em 'retweeted_status-text'.
# Armazene o nome de tela do usuário retuitado em 'retweeted_status-user-screen_name'.
# Armazene o texto retuitado em 'retweeted_status-text'.

for dj in data_json:
    fh = open(dj, 'r', encoding = 'utf-8')
    tweets_json = fh.read().split("\n")    
    tweets_json = list(filter(len, tweets_json))    
    for tweet in tweets_json:
        tweet_obj = json.loads(tweet)        
        tweet_obj['user-screen_name'] = tweet_obj['user']['screen_name']            
        if 'extended_tweet' in tweet_obj:            
            tweet_obj['extended_tweet-full_text'] = tweet_obj['extended_tweet']['full_text']    
        if 'retweeted_status' in tweet_obj:            
            tweet_obj['retweeted_status-user-screen_name'] = tweet_obj['retweeted_status']['user']['screen_name']            
            tweet_obj['retweeted_status-text'] = tweet_obj['retweeted_status']['text']
        if 'quoted_status' in tweet_obj:            
            tweet_obj['quoted_status-user-screen_name'] = tweet_obj['quoted_status']['user']['screen_name']            
            tweet_obj['quoted_status-text'] = tweet_obj['quoted_status']['text']            
        tweets.append(tweet_obj)

data_frame_tweets = pd.DataFrame(tweets) # retonra uma datafram do pandas

# data_frame_tweets.head()
# data_frame_tweets.info()

# Verifica se uma palavra está em um texto de conjunto de dados do Twitter.
# Verifica texto e tweet estendido (tweets com mais de 140 caracteres) para tweets, retweets e tweets de citações.
# Retorna uma série de pandas lógico.
def check_word_in_tweet(word, data):    
    contains_column = data['text'].str.contains(word, case = False)
    contains_column |= data['extended_tweet-full_text'].str.contains(word, case = False)
    contains_column |= data['quoted_status-text'].str.contains(word, case = False) 
    contains_column |= data['retweeted_status-text'].str.contains(word, case = False) 
    return contains_column

def check_keywords_to_track(keywords_to_track):
    global data_frame_tweets
    for x in keywords_to_track:           
        mentions = check_word_in_tweet(x, data_frame_tweets) # Encontre menções da palavra-chave em todos os campos de texto
        print("Proporção de tweets #" + x + "", np.sum(mentions) / data_frame_tweets.shape[0])

check_keywords_to_track(keywords_to_track)

data_frame_tweets['created_at'] = pd.to_datetime(data_frame_tweets['created_at']) # Convertendo a coluna created_at para o objeto np.datetime

data_frame_tweets = data_frame_tweets.set_index('created_at') # Defina o índice de data_frame_tweetss para a coluna created_at

def create_columns_in_the_dataset(keywords_to_track):
    global data_frame_tweets
    for x in keywords_to_track:        
        data_frame_tweets[x] = check_word_in_tweet(x, data_frame_tweets) # gera uma coluna de palavra-chave no dataframe

create_columns_in_the_dataset(keywords_to_track)

hashTags = []
std_colors = ['green', 'blue', 'red', 'purple', 'orange', 'brown', 'pink', 'gray', 'olive', 'cyan']

def average_of_mentions_by_minute(keywords_to_track):
    global data_frame_tweets
    global hashTags
    global std_colors
        
    for i in range(len(keywords_to_track)):        
        mymean = data_frame_tweets[keywords_to_track[i]].resample('1 min').mean() # Média da coluna de palavras-chave por dia
        plt.plot(mymean.index.minute, mymean, color = std_colors[i]) # exibe a palavra chave média por minuto
    
    for i in range(len(keywords_to_track)):
        hashTags.append("#"+keywords_to_track[i])    
    
    plt.xlabel('Minutos')
    plt.ylabel('Media')
    plt.title('Menções ao longo do tempo')    
    plt.legend(hashTags)
    plt.show()

average_of_mentions_by_minute(keywords_to_track)

# data_frame_tweets.info()

sia = SentimentIntensityAnalyzer()
polarity_scores = data_frame_tweets['text'].apply(sia.polarity_scores) # cria score de sentimentos
sentiment = polarity_scores.apply(lambda x: x['compound'])
results = []

def average_sentiment_score_by_minute(hashTags):
    global data_frame_tweets
    global results
    global sentiment    
    for i in range(len(hashTags)):        
        results.append(sentiment[ check_word_in_tweet(hashTags[i], data_frame_tweets) ].resample('1 min').mean()) # médias do score de sentimento para cada hashtag
        
average_sentiment_score_by_minute(hashTags)

def plot_avegare_sentiment_analysis(results):
    global std_colors
    global hashTags    
    for i in range(len(results)):        
        plt.plot(results[i].index.minute, results[i], color = std_colors[i]) # exibe média dos sentimentos das hashtags por minuto

    plt.xlabel('Minutos')
    plt.ylabel('Score')
    plt.title('Sentimentos ao logo do tempo')    
    plt.legend(hashTags)
    plt.show()

plot_avegare_sentiment_analysis(results)