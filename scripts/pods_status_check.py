#!/usr/bin/env python3
import subprocess
import json
import time
import requests,json,hmac,urllib,time,base64,hashlib,sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=82969d8d73bf3afdd40e42f087437bf1fe7117d7e99758396fb7e7fbd2cbd2fe"
secret = 'SEC8d2d6fb31ef203f0fc1bebc49627ac3ac85d0870d35f566ebf7562067fc4ee1a'

# 加签
def Signature_Url(webhook_url, secret):
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    Post_Url = ("%s&timestamp=%s&sign=%s" % (webhook_url, timestamp, sign))
    return Post_Url


# 发送消息
def DingMessage(Title, Content, People):
    Post_Url = Signature_Url(webhook_url, secret)
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    message ={
        "msgtype": "markdown",
        "markdown": {
            "title": Title,
            "text": Content
        },
        "at": {
            "isAtAll": People
        }
    }
    requests.post(url=Post_Url,data=json.dumps(message),headers=header)


def check_pods_status():
    try:
        # 获取 pods 的 JSON 格式输出
        output = subprocess.check_output(["kubectl", "get", "pods", "-o", "json"], universal_newlines=True)
        data = json.loads(output)
        pods = data.get("items", [])
        total = len(pods)
        
        # 检查每个 Pod 的状态是否为 "Running"
        abnormal = [pod for pod in pods if pod.get("status", {}).get("phase") != "Running"]
        abnormal_count = len(abnormal)
        
        if abnormal_count > 0 or total < 200:
            logging.warning(f"WARNING: {abnormal_count} out of {total} pods are not Running.")
            # send ding message
            msgstr = f"[ERROR] DCLM pods 状态异常 @许飞 \n\n"
            msgstr += f"{abnormal_count} out of {total} pods are not Running."
            DingMessage("[ERROR] pods 状态异常", msgstr, False)
            # 你也可以打印出具体的 Pod 名称和状态
            for pod in abnormal:
                name = pod.get("metadata", {}).get("name")
                phase = pod.get("status", {}).get("phase")
                logging.warning(f" - Pod {name}: {phase}")
            return False
        else:
            logging.info(f"All {total} pods are Running.")
            return True
    except subprocess.CalledProcessError as e:
        logging.error("Error executing kubectl command:", e)
        return True
    except Exception as e:
        logging.error("Error checking pod status:", e)
        return True



def main():
    interval_seconds = 60  # 每60秒检测一次
    while True:
        print(f"检测时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        if not check_pods_status():
            break
        time.sleep(interval_seconds)

    logging.info("Exited!")

if __name__ == '__main__':
    main()
