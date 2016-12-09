from __future__ import print_function
import boto3
import os
import sys
import ftplib
import paramiko


ip = os.environ['ip']
username = os.environ['user']
remote_directory = os.environ['remoteDirectory']
key_file = os.environ['privatekeyfilename']
keybucket = os.environ['key_bucket']
s3_client = boto3.client('s3')

def FTPTransfer(sourcekey):
    os.chdir("/tmp/")
    try:
        key = paramiko.RSAKey.from_private_key_file(key_file)
        t = paramiko.Transport((ip,22))
        host=ip
        print ('Connecting to ' + host)
        t.connect(username='ftpssh',pkey=key)
        print('Connected to '+ host)
        sftp = paramiko.SFTPClient.from_transport(t)
        if remote_directory.strip() <> '': 
            sftp.chdir(path=remote_directory)
            print('Working directory changed to {}'.format(remote_directory))
        ref=sftp.put(sourcekey,sourcekey)
        print ('file {} transmitted!!!'.format(sourcekey))
        t.close()
    except Exception,e:
        print('SFTP Failed - ',e)
    print('SFTP Connection closed')
    

def lambda_handler(event, context):
    print('In the handler function...')
    
    for record in event['Records']:
        sourcebucket = record['s3']['bucket']['name']
        sourcekey = record['s3']['object']['key'] 
        destbucket = os.environ['destbucket']
        print('all env variables read')
#Download the file to /tmp/ folder        
        download_path = '/tmp/'+sourcekey
        s3_client.download_file(sourcebucket, sourcekey, download_path)
#Download the Key file from destination bucket
        download_key_path = '/tmp/'+key_file
        s3_client.download_file(keybucket, key_file, download_key_path)
        
#upload the file to the destination bucket
        s3_client.upload_file(download_path, destbucket, sourcekey)
        print('file copied to another bucket - {}'.format(destbucket))    

#FTP the file to the FTP host
        FTPTransfer(sourcekey)

#We don't need the file in /tmp/ folder anymore
        os.remove(sourcekey)
        print('File Deleted from /tmp/ folder')

#Now, that the file is transmitted and backedup, remove the file from the Source bucket        
        s3_client.delete_object(Bucket=sourcebucket, Key=sourcekey)
        print('Deleted file {} from Source bucket {}'.format(sourcekey,sourcebucket))
        
        return
        
