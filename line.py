import requests

def sendMsgToLine(topic, partition, offset, key, value):
  url = 'https://notify-api.line.me/api/notify'
  token = 'lVmWwPWJuZ11XQmbfB3nSElJYPp3RplGeYkOQyo1X5c'
  headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+ token}
  msg = "%s:%d:%d: key=%s value=%s" % (topic, partition, offset, key, value)
  r = requests.post(url, headers=headers, data = {'message':msg})
  return r
