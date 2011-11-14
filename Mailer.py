"""
Created on Oct. 12 2011

@author: Jason MacWilliams
@dependencies: lxml

@TODO: email integration:waiting on server
"""

import subprocess

def sendEmail(addrs, subject, message):
    print("Sending email (%s) to addresses %s" % (subject, addrs))
    subprocess.Popen('echo "%s" | mailx -s "%s" %s' % (message, subject, addrs), shell=True, executable="/bin/bash")

    # cmd = echo "message" | mailx -s "subject" addresses

    # XXX: we might want to attach the logfile or something else here.  In that case the order of
    # the print statement and the sendmail should be reversed so the print statement doesn't appear
    # in the log
