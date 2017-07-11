# -*- coding: utf-8 -*-

__author__ = 'liuyichao'

import smtplib
from email.mime.text import MIMEText
import socket
import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase
import os.path

def mail_to(smtp_server,sender,user,passwd,receviers,subject,text,file_name):
    server = smtplib.SMTP(smtp_server)
    #server.login(user,passwd)
    main_msg = email.MIMEMultipart.MIMEMultipart()
    text_msg = email.MIMEText.MIMEText(text)
    main_msg.attach(text_msg)
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)

    data = open(file_name, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read( ))
    data.close( )
    email.Encoders.encode_base64(file_msg)

    basename = os.path.basename(file_name)
    file_msg.add_header('Content-Disposition',
        'attachment', filename = basename)
    main_msg.attach(file_msg)

    main_msg['From'] = sender
    main_msg['To'] = ','.join(receviers)
    main_msg['Subject'] = subject
    main_msg['Date'] = email.Utils.formatdate( )

    fullText = main_msg.as_string( )

    server.sendmail(sender, receviers, fullText)
    server.quit()

'''

SENDER = 'liuyichao@yidian-inc.com'
SMTP_SERVER = 'smtp.yidian-inc.com'
TO = 'liuyichao@yidian-inc.com'
file_name = 'star_list.conf'

'''





if __name__ == '__main__':
    mail_to('smtp.yidian.com','liuyichao@yidian-inc.com',['liuyichao'],'liuyichaoLYC285456','liuyichao@yidian-inc.com','测试','只是测试','pipline.py')