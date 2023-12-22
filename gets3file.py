import os
import fcntl
import sys
import socket
import gzip
from datetime import datetime, timedelta, timezone
import pandas as pd

lock_file = "script_lock.lock"

# S3 bucket setting
status_file_path = 'status_file.csv'
logs_dir_suffix = 'logs/'
syslog_server = 'localhost'
syslog_port = 514


# Main processing
def main():
    # 1. Exclusive control
    lock_fd = acquire_lock()
    if lock_fd is None:
        print("The script is already running. Exiting.")
        sns_topics("gets3file.py: The script is already running. Exiting.", "test")
        sys.exit(1)

    try:
        # 2. Define start/end time based on proceeded file.
        #   start_time : latest time - 1hour
        #   end_time   : current time
        #   time_list  : time list with 5 interval between start_time and end_time
        proceeded_status = []
        if not os.path.exists(status_file_path) or not os.path.getsize(status_file_path) > 0:
            start_time = datetime.now()
        else:
            with open(status_file_path, "r") as file:
                for line in file:
                    proceeded_status.append(line.strip())
            proceeded_time_list = []
            for proceeded_file in proceeded_status:
                date_str = proceeded_file.split("/")[1:6]
                date_object = datetime.strptime('/'.join(date_str), "%Y/%m/%d/%H/%M")
                proceeded_time_list.append(date_object)
            start_time = max(proceeded_time_list) - timedelta(hours=1)
        print(start_time)
        # end_time = datetime.now() # debug
        end_time = datetime(2023, 9, 1, 19, 30)  # debug
        current_time = start_time
        time_list = []
        while current_time <= end_time:
            time_list.append(current_time.strftime("%Y/%m/%d/%H/%M"))
            current_time += timedelta(minutes=5)
        print(time_list)
        # 3. Define file path based on time_list
        # file_path : absolute file path under specific time directory
        file_path = []
        for time_target in time_list:
            directory_path = logs_dir_suffix + time_target
            if os.path.exists(directory_path) and os.path.isdir(directory_path):
                file_list = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
                file_path_list = [os.path.join(directory_path, file) for file in file_list]
                for file_path_target in file_path_list:
                    file_path.append(file_path_target)
        print(file_path)

        # 4. Define target file path
        # target_files : (absolute file path under specific time directory) - (proceeded status file)
        target_files = [element for element in file_path if element not in proceeded_status]
        print(target_files)

        # 5. Read file and send syslog
        try:
            for gz_file in target_files:
                with gzip.open(gz_file, 'rt') as f:
                    logs = f.read()
                    print(logs)
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(logs.encode('utf-8'), ("localhost", 514))
                print("Send Syslog")
        except Exception:
            sns_topics("gets3file.py: FileOpen or Syslog Error", "test")

        # 6. Add data to proceeded file.
        with open(status_file_path, 'a') as file:
            for line in target_files:
                file.write(line + '\n')

    finally:
        release_lock(lock_fd)


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


def sns_topics(subject, body):
    sns_client = boto3.client('sns', region_name='YOUR_REGION')
    topic_arn = 'arn:aws:sns:YOUR_REGION:YOUR_ACCOUNT_ID:YOUR_TOPIC_NAME'
    custom_message = body
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=custom_message,
            Subject=subject  # オプションの件名
        )
        print(f"Message sent successfully. Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending message: {e}")


if __name__ == '__main__':
    main()
