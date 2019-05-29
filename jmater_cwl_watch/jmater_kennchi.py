import os
import sys
import json
import boto3
import zlib
import base64
import datetime
import urllib.request

## S3 get list
def getList(fileName, s3Bucket, filePath):

    s3 = boto3.resource('s3')

    s3.Bucket(s3Bucket).download_file(fileName, filePath)
    with open(filePath) as f:
        lines = json.load(f)
        return lines

def getThreadName(log_data):

    threadName = log_data.split()
    if "summary" != threadName[0]:
        gl_threadName0 = threadName[0]
        #gl_threadName1 = threadName[1]
        return gl_threadName0[-9:]
    else:
        pass

## post slack
def post_slack(log_data, log_url, contact):

    SLACK_POST_URL = contact['SLACK']
    channnel = '#rds'
    method = "POST"
    
    threadName = getThreadName(log_data)

    message = "シナリオ({})でエラーとなりました。".format(threadName) + \
        "\nシナリオエラー発生明細行は以下です:" + \
        "\n" + str(log_data) + \
        "\n" + str(log_url)

    send_data = {
        "username": "scenario-",
        "text": message,
        "channel": channnel
    }

    send_text = ("payload=" + json.dumps(send_data)).encode('utf-8')
    request = urllib.request.Request(
        SLACK_POST_URL,
        data=send_text,
        method=method
    )
    with urllib.request.urlopen(request) as response:
        response_body = response.read().decode('utf-8')

    return response_body

## post mail
def post_sns(log_data, log_url, contact, errorcode):

    topic_arn = contact['Mail']
    sns = boto3.client('sns')
    
    threadName = getThreadName(log_data)

    title = "シナリオ検知エラー通知-" + threadName + "-" + errorcode
    sns_message = "シナリオ({})でエラーとなりました。".format(threadName) + \
        "\nシナリオエラー発生明細行は以下です:" + \
        "\n" + str(log_data) + \
        "\n" + str(log_url)

    responses = sns.publish(
        TopicArn = topic_arn,
        Message = sns_message,
        Subject = title
    )

## main
def lambda_handler(event, context):

    # get logs
    data_json = json.loads(zlib.decompress(base64.b64decode(event['awslogs']['data']), 16+zlib.MAX_WBITS))
    log_json = json.loads(json.dumps(data_json, ensure_ascii=False))
    log_grpname = log_json["logGroup"]
    log_stream = log_json["logStream"]

    # log stream url
    region = context.invoked_function_arn.split(":")[3]
    log_url = "https://"+str(region)+".console.aws.amazon.com/cloudwatch/home?region="+str(region)+"#logEventViewer:group="+str(log_grpname)+";stream="+str(log_stream)

    # s3 param
    fileName = "scenario01.json"
    s3Bucket = "list-jmeter-scenario-err"
    filePath = '/tmp/' + fileName

    # read list and push notification
    listJson = getList(fileName, s3Bucket, filePath)
    for mess in log_json["logEvents"]:
        log_data = mess['message']
        print(log_data)
        for k, v in listJson.items():
            for x in v:
                contact = x['contact']
                errorcode = x['errorcode']
                print (contact)
                print (errorcode)
                print (log_data)
                if errorcode in log_data:
                    print ("true")
                    if "Mail" not in contact:
                        print ("SLACK")
                        post_slack(log_data, log_url, contact)
                    elif "SLACK" not in contact:
                        print ("Mail")
                        post_sns(log_data, log_url, contact, errorcode)
                    else:
                        print ("SLACK, Mail")
                        post_slack(log_data, log_url, contact)
                        post_sns(log_data, log_url, contact, errorcode)
                else:
                    print ("false")


