import sys, os, hashlib, hmac
from base64 import b64encode, decode
from datetime import datetime
from uuid import uuid4
import websocket
import threading
import json

# ************************** Reference **************************
# https://docs.aws.amazon.com/appsync/latest/devguide/real-time-websocket-client.html
# https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
# https://docs.aws.amazon.com/apigateway/api-reference/signing-requests/
# ************************** Reference **************************

# The client connect to wss_url through websocket protocol 
wss_url = 'wss://vlvwhsddczatlel4l4b4k4ygri.appsync-realtime-api.cn-north-1.amazonaws.com.cn/graphql'

# The client will authenticate itself from http_url through http protocol
# Query and mutation will be ran within http_url endpoint
http_url = 'https://vlvwhsddczatlel4l4b4k4ygri.appsync-api.cn-north-1.amazonaws.com.cn/graphql'

host = http_url.replace('https://','').replace('/graphql','')
# IAM authentication method
method = 'POST'
region = 'cn-north-1'
service = 'appsync'
access_key = 'xxxxxxxxxx'
secret_key = 'xxxxxxxxxx'
canonical_uri = '/graphql/connect' 
canonical_querystring = ''
# payload type has to be string
payload = '{}'
accept = "application/json, text/javascript"
content_encoding = "amz-1.0"
content_type = "application/json; charset=UTF-8"

# ********************************** AWS V4 Signature **********************************
def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def getSignatureKey(key, dateStamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

def gen_auth(servic_name, region_name, ak, sk, canonical_uri, canonical_querystring, payload):
    t = datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d') # Date w/o time, used in credential scope
    payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + amzdate + '\n'
    signed_headers = 'host;x-amz-date'
    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = datestamp + '/' + region + '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' +  amzdate + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    signing_key = getSignatureKey(secret_key, datestamp, region, service)
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
    authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' +  'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature
    return ({'auth_date':amzdate,'auth_header':authorization_header})

# *************************** AppSync: websocket ***************************
# Generate IAM authentication header
auth = gen_auth(service,region,access_key,secret_key,canonical_uri,canonical_querystring,payload)

iam_header = {
    'accept': accept,
    'content-encoding': content_encoding,
    'content-type': content_type,
    'host': host,
    'x-amz-date': auth['auth_date'],
    'Authorization': auth['auth_header']
}

# GraphQL subscription Registration object
GQL_SUBSCRIPTION = json.dumps({
        'query': 'subscription MySubscription {subscribeToEventComments(eventId: "0534ad04-fd29-49d6-815a-fa64a80979c1") {commentId content createdAt eventId } }',
        'variables': {}
})


# Set up Timeout Globals
timeout_timer = None
timeout_interval = 10
# Subscription ID (client generated)
SUB_ID = str(uuid4())

# Calculate UTC time in ISO format (AWS Friendly): YYYY-MM-DDTHH:mm:ssZ
def header_time():
    return datetime.utcnow().isoformat(sep='T',timespec='seconds') + 'Z'

# Encode Using Base 64
def header_encode( header_obj ):
    return b64encode(json.dumps(header_obj).encode('utf-8')).decode('utf-8')

# reset the keep alive timeout daemon thread
def reset_timer( ws ):
    global timeout_timer
    global timeout_interval

    if (timeout_timer):
        timeout_timer.cancel()
    timeout_timer = threading.Timer( timeout_interval, lambda: ws.close() )
    timeout_timer.daemon = True
    timeout_timer.start()

# Socket Event Callbacks, used in WebSocketApp Constructor
def on_message(ws, message):
    global timeout_timer
    global timeout_interval

    print('### message ###')
    print('<< ' + message)

    message_object = json.loads(message)
    message_type   = message_object['type']

    if( message_type == 'ka' ):
        reset_timer(ws)

    elif( message_type == 'connection_ack' ):
        timeout_interval = int(json.dumps(message_object['payload']['connectionTimeoutMs']))
        payload = GQL_SUBSCRIPTION
        canonical_uri ='/graphql'
        sub_auth = gen_auth(service,region,access_key,secret_key,canonical_uri,canonical_querystring,payload)
        register = {
            'id': SUB_ID,
            'payload': {
                'data': GQL_SUBSCRIPTION,
                'extensions': {
                    'authorization': {
                        'host': host,
                        'Authorization': sub_auth['auth_header'],
                        'x-amz-date': sub_auth['auth_date']
                    }
                }
            },
            'type': 'start'
        }
        start_sub = json.dumps(register)
        print('>> '+ start_sub )
        ws.send(start_sub)

    elif(message_type == 'data'):
        deregister = {
            'type': 'stop',
            'id': SUB_ID
        }
        end_sub = json.dumps(deregister)
        print('>> ' + end_sub )
        ws.send(end_sub)

    elif(message_object['type'] == 'error'):
        print ('Error from AppSync: ' + message_object['payload'])
    
def on_error(ws, error):
    print('### error ###')
    print(error)

def on_close(ws):
    print('### closed ###')

def on_open(ws):
    print('### opened ###')
    init = {
        'type': 'connection_init'
    }
    init_conn = json.dumps(init)
    print('>> '+ init_conn)
    ws.send(init_conn)

if __name__ == '__main__':
    # Uncomment to see socket bytestreams
    #websocket.enableTrace(True)

    # Set up the connection URL, which includes the Authentication Header
    #   and a payload of '{}'.  All info is base 64 encoded
    connection_url = wss_url + '?header=' + header_encode(iam_header) + '&payload=e30='

    # Create the websocket connection to AppSync's real-time endpoint
    #  also defines callback functions for websocket events
    #  NOTE: The connection requires a subprotocol 'graphql-ws'
    print( 'Connecting to: ' + connection_url )

    ws = websocket.WebSocketApp( connection_url,
                            subprotocols=['graphql-ws'],
                            on_open = on_open,
                            on_message = on_message,
                            on_error = on_error,
                            on_close = on_close,)

    ws.run_forever()