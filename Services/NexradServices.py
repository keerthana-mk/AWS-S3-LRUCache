import logging
from concurrent.futures import thread

import requests as rq
import json


class NexradServices:
    def __init__(self, domain):
        self.domain = domain
        self.session = rq.session()

    def jsonprint(self, obj):
        text = json.dumps(obj, indent=4)
        # print(text)
        return text

    def nexrad_all_api(self):
        requestUrl = self.domain + "/nexrad/all"
        try:
            response = self.session.get(requestUrl)
            response_body = response.json()
        except Exception as e:
            logging.ERROR(e)
            raise Exception("error while downloading Nextrad bulk data: {}".format(e.val))
        return self.jsonprint(response_body)

    def nexrad_api(self, parameters):
        requestUrl = self.domain + "/nexrad"
        try:
            response = self.session.get(requestUrl, params=parameters)
            response_body = response.json()
            print(response.status_code, response_body)
            if response.status_code == 200:
                if response_body['status'] == 2:
                    return True, response_body['dataS3Key']
                else:
                    while (1):
                        thread.sleep(2000)
                        res = self.session.get(requestUrl, params=parameters)
                        res_body = res.json()
                        if res.status_code == 200 and res_body['status'] == 2:
                            return True, res_body['dataS3Key']
                        elif res.status_code == 404:
                            return False, None
            else:
                logging.info("Requested data not found")
                return False, None
        except Exception as e:
            logging.error(e)
            raise Exception("Error while downloading data from Nexrad for given parameters: {}".format(e.val))

    def cache_nexrad_data(self, capacity):
        bulk_data = self.nexrad_all_api()
        bulk_data = json.loads(bulk_data)
        bulk_data.sort(key=lambda x: x['lastAccessTime'])
        valid_s3_keys = []
        # print((bulk_data))
        try:
            for user_req in bulk_data:
                # print(user_req['status'])
                if user_req['status'] == 2:
                    valid_s3_keys.append(user_req['dataS3Key'])
            num_extra_data = len(valid_s3_keys) - capacity
            if num_extra_data > 0:
                # delete S3 based on least access time
                return valid_s3_keys[:num_extra_data]
            else:
                return []
        except Exception as e:
            logging.error(e)
            raise Exception(" Error while caching nexrad data :{}".format(e.val))


