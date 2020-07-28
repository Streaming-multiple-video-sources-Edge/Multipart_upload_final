import boto3
import sys, os
import threading
import logging
import base64

DEFAULT_BUCKETNAME = "myBucket"
DEFAULT_ENDPOINT = "http://ceph-route-rook-ceph.apps.neeha-ocp.shiftstack.com"
DEFAULT_ACCESS = "SFQ4MzE0SkxKNFRFOUNMTDZPV04="
DEFAULT_SECRET = "MFV4VUFkb3p3RW5jWXVNZjk2S2lKOXdWTGJMaEdkUVNMQngzb2hMUA=="


s3 = None
s3r = None
mpu = None              # Multipart upload handle

#
# Thread (safe) function responsible of uploading a part of the file
#
def upload_part_r(partid, part_start, part_end, thr_args):
        filename = thr_args['FileName']
        bucket = thr_args['BucketName']
        upload_id = thr_args['UploadId']

        logging.info("%d: >> Uploading part %d", partid, partid)
        logging.info("%d: --> Upload starts at byte %d", partid, part_start)
        logging.info("%d: --> Upload ends at byte %d", partid, part_end)

        f = open(filename, "rb")
        logging.info("%d: DEBUG: Seeking offset: %d", partid, part_start)
        logging.info("%d: DEBUG: Reading size: %d", partid, part_end - part_start)
        f.seek(part_start, 0)
        # XXX: Would the next read fail if the portion is too large?
        data = f.read(part_end - part_start + 1)

        # DO WORK HERE
        # TODO:
        # - Variables like mpu, Bucket, Key should be passed from caller -- DONE
        # - We should collect part['ETag'] from this part into array/list, so we must synchronize access
        #   to that list, this list is then used to construct part_info array to call .complete_multipart_upload(...)
        # TODO.
        #
        # NOTES:
        # - Since part id is zero based (from handle_mp_file function), we add 1 to it here as HTTP parts should start
        #   from 1
        part = s3.upload_part(Bucket=DEFAULT_BUCKETNAME, Key=filename, PartNumber=partid+1, UploadId=upload_id, Body=data)

        # Thread critical variable which should hold all information about ETag for all parts, access to this variable
        # should be synchronized.
        lock = thr_args['Lock']
        if lock.acquire():
                thr_args['PartInfo']['Parts'].append({'PartNumber': partid+1, 'ETag': part['ETag']})
                lock.release()

        f.close()
        logging.info("%d: -><- Part ID %d is ending", partid, partid)
        return

#
# Part size calculations.
# Thread dispatcher
#
def handle_mp_file(bucket, filename, nrparts):

        print(">> Uploading file: " + filename + ", nr_parts = " + str(nrparts))

        fsize = os.path.getsize(filename)
        print("+ %s file size = %d " % (filename, fsize))

        npart = nrparts
        # do the part size calculations
        while(1):
            part_size = int(fsize / npart)
            print("+ standard part size = " + str(part_size) + " bytes")
            if (part_size > 5242880):
                print("The new bucket size is " + str(npart) + "because the parts have to be minimum 5 MB")
                nrparts = npart
                break ;
            npart = npart - 1

        print("$$$$$$$$$$$$$$$$$$$$$$$")
        print(bucket)
        print(filename)
        # Initiate multipart uploads for the file under the bucket
        mpu = s3.create_multipart_upload(Bucket=DEFAULT_BUCKETNAME, Key=filename)

        threads = list()
        thr_lock = threading.Lock()
        thr_args = { 'PartInfo': { 'Parts': [] } , 'UploadId': mpu['UploadId'], 'BucketName': bucket, 'FileName': filename,
                'Lock': thr_lock }

        for i in range(nrparts):
                print("++ Part ID: " + str(i))

                part_start = i * part_size
                part_end = (part_start + part_size) - 1

                if (i+1) == nrparts:
                        print("DEBUG: last chunk, part-end was/will %d/%d" % (part_end, fsize))
                        part_end = fsize

                print("DEBUG: part_start=%d/part_end=%d" % (part_start, part_end))

                thr = threading.Thread(target=upload_part_r, args=(i, part_start, part_end, thr_args, ) )
                threads.append(thr)
                thr.start()
        # Wait for all threads to complete
        for index, thr in enumerate(threads):
                thr.join()
                print("%d thread finished" % (index))

        part_info = thr_args['PartInfo']
        for p in part_info['Parts']:
                print("DEBUG: PartNumber=%d" % (p['PartNumber']))
                print("DEBUG: ETag=%s" % (p['ETag']))

        print("+ Finishing up multi-part uploads")
        s3.complete_multipart_upload(Bucket=DEFAULT_BUCKETNAME, Key=filename, UploadId=mpu['UploadId'], MultipartUpload=thr_args['PartInfo'])
        return True


### MAIN ###

if __name__ == "__main__":
        #bucket = 'test'                 # XXX FIXME: Pass in arguments

        if len(sys.argv) != 3:
                print("usage: %s <filename to upload> <number of threads/parts>" % (sys.argv[0]))
                sys.exit(1)

        # Filename: File to upload
        # NR Parts: Number of parts to divide the file to, which is the number of threads to use
        filename = sys.argv[1]
        nrparts = int(sys.argv[2])

        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

        # Initialize the connection with Ceph RADOS GW
        s3 = boto3.client(service_name = 's3', use_ssl = False, verify = False,
                                endpoint_url = DEFAULT_ENDPOINT,
                                aws_access_key_id = base64.decodebytes(bytes(DEFAULT_ACCESS,'utf-8')).decode('utf-8'),
                                aws_secret_access_key = base64.decodebytes(bytes(DEFAULT_SECRET,'utf-8')).decode('utf-8'),)
        s3r = boto3.resource(service_name = 's3', use_ssl = False, verify = False, endpoint_url = DEFAULT_ENDPOINT,
                                aws_access_key_id = base64.decodebytes(bytes(DEFAULT_ACCESS,'utf-8')).decode('utf-8'),
                                aws_secret_access_key = base64.decodebytes(bytes(DEFAULT_SECRET,'utf-8')).decode('utf-8'),)

        response = s3.list_buckets()
        # Get a list of all bucket names from the response
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        # Print     out the bucket list
        print("Initial bucket List: %s" % buckets)

        #s3r.Bucket("MyBucket").objects.all().delete()
        #s3.delete_bucket(Bucket="MyBucket")

        print("Trying to make 'mybucket'")
        if DEFAULT_BUCKETNAME not in buckets:
            s3.create_bucket(Bucket=DEFAULT_BUCKETNAME)
        else:
            print("Bucket " + DEFAULT_BUCKETNAME + " already exists, deleting and recreating")
            s3r.Bucket(DEFAULT_BUCKETNAME).objects.all().delete()
            s3.delete_bucket(Bucket=DEFAULT_BUCKETNAME)
            s3.create_bucket(Bucket=DEFAULT_BUCKETNAME)

        response = s3.list_buckets()
        #Get all buckets 
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        # Print out the bucket list
        print("Updated bucket List: %s" % buckets)


        handle_mp_file(buckets, filename, nrparts)

### END ###

