from __future__ import print_function
import boto3
import os
import sys
import ftplib

ip = os.environ['ip']
username = os.environ['user']
password = os.environ['password']
remote_directory = os.environ['remoteDirectory']

s3_client = boto3.client('s3')

def FTPTransfer(sourcekey):
#If we don't change the current working directory to /tmp/, while doing the FTP, LAMBDA tries to create the file
#in the /tmp folder in the host as well - if we give download_path as the filename in storlines function.
    os.chdir("/tmp/")
    ftp = ftplib.FTP(ip)
    ftp.login(username, password)
    print(ftp.getwelcome())
#Based on the Host FTP server, change the following statement to True or False        
    ftp.set_pasv(True)
    print('FTP set to Passive Mode')
#Change the working directory in the host
    if remote_directory.strip() <> '':
        ftp.cwd(remote_directory)
        print('Working directory in host changed to {}'.format(remote_directory))
#Initiate File transfer     
#Assumption is that the input file will always be a text file. In case if the file is other than text
#use the following commented statements
#   file = open(sourcekey,"rb")
#   ftp.storbinary('STOR ' + sourcekey, file)
    file = open(sourcekey,"r")
    ftp.storlines('STOR ' + sourcekey, file)
    print('File transmitted!!!')
    
    

def lambda_handler(event, context):
    for record in event['Records']:
        sourcebucket = record['s3']['bucket']['name']
        sourcekey = record['s3']['object']['key'] 
        destbucket = os.environ['destbucket']

#Download the file to /tmp/ folder        
        download_path = '/tmp/'+sourcekey
        s3_client.download_file(sourcebucket, sourcekey, download_path)
        
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
        