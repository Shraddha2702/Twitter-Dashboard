import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = '1410078775-quutjeXVc4wYkafTPj4V7jfbdBGuS8ZLt4CRs2t'
ACCESS_SECRET = '5QNZGIZH9g8rL3D0g09fGF8dLtbFdJoppUm3KjZtoQRCJ'
CONSUMER_KEY = 'Nb0lz1ZSgVoqGBTYOP0MYklVv'
CONSUMER_SECRET = 'LeXYh3zS9XHobpzYe6TFc8chlwnuMYaad8LBIFB0KLaQXFP4xh'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            line = line.decode('utf-8')
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text'] + '\n'
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(bytearray(tweet_text, 'utf8'))

            #line = line.decode('utf-8')
            #ull_tweet = json.loads(line)

            #if full_tweet['entities']:
                #print(full_tweet['entities'])
            #    media = full_tweet['entities']['media']
            #    if len(media) > 0:
            #        print(media[0]['media_url'])
            #        print ("------------------------------------------")
            #        tcp_connection.send(bytearray(media[0]['media_url'], 'utf8'))
        except:
            e = sys.exc_info()
            print(e)
            print ("------------------------------------------")


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    #query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 8008
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)