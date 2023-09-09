import os
import fcntl
import sys
import boto3
import configparser
import socket
import gzip
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

# AWS S3クライアントのセットアップ
s3 = boto3.client('s3')

# ロックファイルを設定
lock_file = "script_lock.lock"

# メール設定
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_user = 'your-email@gmail.com'  # 送信元メールアドレス
smtp_password = 'your-email-password'  # 送信元メールアドレスのパスワード
smtp_receiver = 'receiver-email@example.com'  # 受信先メールアドレス

# S3のバケット設定
source_bucket = 'your-source-bucket'
source_directory = 'your-source-directory'
config_bucket = 'your-config-bucket'

# Create a lock file and check that no other instance is running
def acquire_lock():
    try:
        lock_fd = open(lock_file, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_fd
    except (IOError, BlockingIOError):
        return None

# Release the lock file
def release_lock(lock_fd):
    if lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

# Send an email in case of failure
def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = smtp_receiver
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, smtp_receiver, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

# S3からファイルの内容を取得し、gzip圧縮を解凍して返す
def get_file_content(bucket_name, file_name):
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        content = obj['Body'].read()
        if file_name.endswith('.gz'):
            content = gzip.decompress(content).decode('utf-8')
        return content
    except Exception as e:
        print(f"Failed to get file content: {str(e)}")
        return None

# Download the config file from S3
def download_config_file():
    try:
        s3.download_file(config_bucket, 'config.ini', '/tmp/config.ini')
        return True
    except Exception as e:
        print(f"Failed to download config file: {str(e)}")
        return False

# Upload the config file to S3
def upload_config_file():
    try:
        s3.upload_file('/tmp/config.ini', config_bucket, 'config.ini')
        return True
    except Exception as e:
        print(f"Failed to upload config file: {str(e)}")
        return False

# Main processing
def main():
    lock_fd = acquire_lock()
    if lock_fd is None:
        print("The script is already running. Exiting.")
        sys.exit(1)

    try:
        # Download the config file from S3
        if download_config_file():
            config_file_path = '/tmp/config.ini'
        else:
            print("Failed to download the config file. Exiting.")
            sys.exit(1)

        # Set the S3 bucket and directory
        source_bucket = 'your-source-bucket'
        source_directory = 'your-source-directory'
        
        # Get a list of files and timestamps from S3
        files_and_timestamps = list_files_in_s3_directory(source_bucket, source_directory)

        # Extract files with timestamps within the last 30 minutes
        threshold = datetime.now(timezone.utc) - timedelta(minutes=30)
        recent_files = [(file, timestamp) for file, timestamp in files_and_timestamps if timestamp >= threshold]

        # Get the list of files to process from the Config file
        config_files = get_config_files(config_file_path)

        # Identify missing files
        missing_files = [file for file, _ in recent_files if file not in config_files]

        # Send Syslog messages
        syslog_server = 'localhost'
        syslog_port = 514

        for file in missing_files:
            syslog_message = f"File {file} is missing in the config."
            send_syslog_message(syslog_server, syslog_port, syslog_message)

            # Add the file name to the Config file
            add_file_to_config(config_file_path, file)

            # Get the file content and send it via Syslog
            file_content = get_file_content(source_bucket, file)
            if file_content:
                send_syslog_message(syslog_server, syslog_port, file_content)

        # Upload the updated Config file to S3
        if upload_config_file():
            print("Config file uploaded to S3.")
        else:
            print("Failed to upload the config file to S3.")

    except Exception as e:
        # Send an email with the error message in case of failure
        subject = "Script Error"
        body = f"An error occurred in the script:\n\n{str(e)}"
        send_email(subject, body)

    finally:
        # Release the lock file
        release_lock(lock_fd)

if __name__ == '__main__':
    main()

