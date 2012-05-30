aws-utility
===========

This is various utilities that I've cobbled together for ease of aws use

s3/s3_multipart_upload.py - Just what is says,  It allows you to upload a file (of any size) to a bucket in s3.  It uses the filechunkIO (https://bitbucket.org/fabian/filechunkio) for in memory file splitting.  It is essentially a duplicate of https://gist.github.com/924094 with some command line arguments.
