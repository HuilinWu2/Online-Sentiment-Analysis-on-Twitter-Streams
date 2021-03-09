import sys

import json
import socket

import tweepy
from tweepy import  OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):
    
    def __init__(self,socket):
        print( 'Tweet listener initialized')
        self.client_socket=socket
        
    
    def on_data(self,data):
        try:
            jsonMessage=json.loads(data)
            message=jsonMessage['text'].encode('utf-8')
            print(message)
            self.client_socket.send(message)
        except BaseException as e:
            print('Error on_data: %s' % str(e))
        return True
    
    def on_error(self,status):
        print(status)
        return True 
    
def connect_to_twitter(connection):
    

    api_key = "FFhys3x3IMQXj3kxkIWfcmLm0"
    api_secret = "gLdinMOLN75vpgWo1VbOyUUzmsil4hvfatTI2FytZ9g5kouXL6"
    access_token = "1342805737043587073-juBuFOmuLbnodV2IrmBIsdxav7DpYk"
    access_token_secret = "wfrvbJgpnSt7BAbNefmuPDOaSELz75RWKATg6cOj2vP4G"
    
    auth=OAuthHandler(api_key,api_secret)
    auth.set_access_token(access_token,access_token_secret)
    
    twitter_stream=Stream(auth,TweetsListener(connection))
    twitter_stream.filter(track=['DHL','dhp'])
    

if __name__=='__main__':
    s=socket.socket()
    host='localhost'
    port=9800
    s.bind((host,port))
    
    print('Listening on port: %s' % str(port))
    
    s.listen(5)
    
    connection,client_address=s.accept()
    
    print ('Received request from : '+str(client_address))
    
    connect_to_twitter(connection)
    
    
    
