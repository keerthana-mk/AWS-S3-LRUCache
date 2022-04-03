import json
import time

from Services.S3Bucket import *
from Services.NexradServices import *
if __name__ == '__main__':
    # s3 = S3Bucket('samplebucket', 'us-east-1', 'XXXX', 'XXXX', 'http://XXX')
    # # s3.create_bucket()
    # # s3.list_existing_buckets()
    # start_time = time.perf_counter()
    # print(s3.multipart_file_upload('SwarmBehaviorData11.zip'))
    # # print(s3.download_all_files())
    # print("total time = {}".format(time.perf_counter() - start_time))
    # print(s3.list_all_bucket_files())
    REQUESTS = json.dumps({
        'request_id' : 'id1',
        'start_time': '',
        'end_time': '',
        'radar_name': ''
    }, indent=4)

    obj = NexradServices('http://XXX')
    # print(obj.nexrad_all_api())
    # parameters = {
    #     'startTime': '2022-04-06T08:38:56.000+00:00',
    #     'endTime': '2022-04-06T08:55:36.000+00:00',
    #     'radarName': 'KABR'
    # }
    # print(obj.nexrad_api(parameters))
    print(obj.cache_nexrad_data(3))