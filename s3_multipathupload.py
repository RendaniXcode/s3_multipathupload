import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def upload_large_file(bucket_name: str, file_path: str, s3_key: str, chunk_size: int = 8 * 1024 * 1024):
    """
    Uploads a large file to S3 in chunks using multipart upload.
    :param bucket_name: Name of the S3 bucket
    :param file_path: Local path to the file
    :param s3_key: S3 key (destination path in the bucket)
    :param chunk_size: Size of each chunk in bytes (default 8MB)
    """
    s3_client = boto3.client('s3')

    try:
        # Initiate multipart upload
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
        upload_id = multipart_upload['UploadId']

        parts = []
        part_number = 1

        with open(file_path, 'rb') as file_obj:
            while True:
                # Read chunk of data
                data = file_obj.read(chunk_size)
                if not data:
                    break

                # Upload part
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                
                # Save part details for completing upload
                parts.append({'ETag': part['ETag'], 'PartNumber': part_number})
                print(f"Uploaded part {part_number}")

                part_number += 1

        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            MultipartUpload={'Parts': parts},
            UploadId=upload_id
        )

        print(f"File uploaded successfully to '{bucket_name}/{s3_key}'")

    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except ClientError as e:
        print(f"Error: {e}")
        # Abort multipart upload on failure
        try:
            s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)
            print("Aborted multipart upload.")
        except Exception as abort_error:
            print(f"Error during abort: {abort_error}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Example usage
if __name__ == "__main__":
    bucket_name = "my-new-nextjs-photo-app"
    file_path = "/home/ec2-user/environment/s3uploader/GMT20241114-170416_Recording.mp4"
    s3_key = "destination/path/in/s3/GMT20241114-170416_Recording.mp4"

    upload_large_file(bucket_name, file_path, s3_key)

