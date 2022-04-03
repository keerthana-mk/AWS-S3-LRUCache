import logging
import os

import boto3
from botocore.exceptions import ClientError


class S3Bucket:
    def __init__(self, bucket_name, region, aws_access_key_id, aws_secret_access_key, endpoint_url):
        self.bucket_name = bucket_name
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key, region_name=region,
                                      endpoint_url=endpoint_url)
        if not self.is_bucket_available():
            logging.info("bucket {} does not exist, creating..".format(self.bucket_name))
            self.create_bucket()

    def is_bucket_available(self):
        all_buckets = self.s3_client.list_buckets()
        for bucket in all_buckets['Buckets']:
            if self.bucket_name not in bucket['Name']:
                return False
            else:
                return True

    def create_bucket(self):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:

            if self.region is None:
                # s3_client = boto3.client('s3')
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                # s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': self.region}
                self.s3_client.create_bucket(Bucket=self.bucket_name,
                                             CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def list_existing_buckets(self):
        # s3 = boto3.client('s3')
        try:
            response = self.s3_client.list_buckets()
            print(response)
            # Output the bucket names
            print('Existing buckets:')
            for bucket in response['Buckets']:
                print(f' {bucket["Name"]}')
        except FileNotFoundError as e:
            logging(e)

    def list_all_bucket_files(self):
        try:
            objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            print("objects ={}".format(objects))
            files = []
            for obj in objects['Contents']:
                files.append(obj['Key'])
        except FileExistsError as e:
            logging(e)
            raise Exception("Error while listing files in the Bucket: {} - {}".format(self.bucket_name, e.val))
        return files

    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        # s3_client = boto3.client('s3')
        try:
            response = self.s3_client.upload_file(file_name, self.bucket_name, object_name)
            print(response)
        except ClientError as e:
            logging.error(e)
            raise Exception(
                "error while uploading the file {} to bucket {}: {}".format(file_name, self.bucket_name, e.val))
        return "File uploaded Successfully"

    # def multipart_file_upload(self, file_name, object_name=None):
    #     if object_name is None:
    #         object_name = os.path.basename(file_name)
    #         print("multipart filename : {}".format(object_name))
    #     try:
    #         response = self.s3_client.upload_file(self.bucket_name, object_name, file_name, Config=self.config)
    #         print("In multipart_file_upload:", response)
    #     except ClientError as e:
    #         logging.error(e)
    #         raise Exception(
    #             "Error while uploading multipart file {} to bucket {}: {}".format(file_name, self.bucket_name, e.val))
    #     return "multipart file uploaded successfully"

    def download_file(self, file_name, object_name=None):
        if object_name is None:
            download_path = '.\Downloads\\'
            object_name = download_path + file_name
            print(object_name)

        try:
            response = self.s3_client.download_file(self.bucket_name, file_name, object_name)
        except ClientError as e:
            raise Exception(
                "Error while downloading the file {} from bucket {}: {}".format(file_name, self.bucket_name, e.val))
        return "File downloaded Successfully"

    def download_all_files(self):
        files_in_bucket = self.list_all_bucket_files()
        print(files_in_bucket)
        try:
            for f in files_in_bucket:
                self.download_file(f)
        except ClientError as e:
            logging.error(e)
            raise Exception(" Error while all file from bucket{}: {}".format(self.bucket_name, e.val))
        return "All files downloaded successfully from bucket " + self.bucket_name
