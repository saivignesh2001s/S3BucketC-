using Amazon.S3.Model;
using Amazon.S3;
using Amazon;
using System.Net.Sockets;
using System.Net;
using Serilog;

namespace S3ClientFactory
{
    public class S3Client
    {
        /// <summary>
        /// ReadS3File
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="S3Bucket"></param>
        /// <param name="S3FileName"></param>
        /// <param name="localPath"></param>
        /// <returns></returns>
        public string ReadS3File(string S3Region, string S3Bucket, string S3FileName, string localPath)
        {
            try
            {
                Log.Information("ReadS3File Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {
                    var GetRequest = new GetObjectRequest
                    {
                        BucketName = S3Bucket,
                        Key = S3FileName
                    };
                    using (var response = s3client.GetObjectAsync(GetRequest).GetAwaiter().GetResult())
                    {
                        using (var filestream = File.Create(localPath))
                        {
                            response.ResponseStream.CopyTo(filestream);
                            Log.Information("ReadS3File Completed");
                            return localPath;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
                return null;
            }
        }
        /// <summary>
        /// ReadS3FileAsStream
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="S3Bucket"></param>
        /// <param name="S3FileName"></param>
        /// <returns></returns>
        public MemoryStream ReadS3FileAsStream(string S3Region, string S3Bucket, string S3FileName)
        {
            MemoryStream stream = new MemoryStream();
            try
            {
                Log.Information("ReadS3FileAsStream Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {
                    var GetRequest = new GetObjectRequest
                    {
                        BucketName = S3Bucket,
                        Key = S3FileName
                    };
                    using (var response = s3client.GetObjectAsync(GetRequest).GetAwaiter().GetResult())
                    {

                        response.ResponseStream.CopyTo(stream);
                        stream.Seek(0, SeekOrigin.Begin);
                        Log.Information("ReadS3FileAsStream Completed");
                        return stream;

                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
                return stream;
            }

        }
        /// <summary>
        /// WriteS3Bucket
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="S3Bucket"></param>
        /// <param name="S3FilePath"></param>
        /// <param name="localPathWithFilename"></param>
        public void WriteS3Bucket(string S3Region, string S3Bucket, string S3FilePath, string localPathWithFilename)
        {
            try
            {
                Log.Information("WriteS3Bucket Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {

                    var request1 = new PutObjectRequest
                    {
                        BucketName = S3Bucket,
                        Key = S3FilePath,
                        FilePath = localPathWithFilename
                    };
                    var putRequest = s3client.PutObjectAsync(request1).GetAwaiter().GetResult();
                }
                Log.Information("WriteS3Bucket Completed");
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
            }

        }
        /// <summary>
        /// TransferOrRenameS3File
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="existingS3Bucket"></param>
        /// <param name="existingS3FilePathWithFileName"></param>
        /// <param name="destinationS3Bucket"></param>
        /// <param name="newS3FilePathWithFileName"></param>
        public void TransferOrRenameS3File(string S3Region, string existingS3Bucket, string existingS3FilePathWithFileName, string destinationS3Bucket, string newS3FilePathWithFileName)
        {
            try
            {
                Log.Information("TransferOrRenameS3File Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {
                    var copyRequest = new CopyObjectRequest
                    {
                        SourceBucket = existingS3Bucket,
                        SourceKey = existingS3FilePathWithFileName,
                        DestinationBucket = destinationS3Bucket,
                        DestinationKey = newS3FilePathWithFileName
                    };
                    var copyRequest1 = s3client.CopyObjectAsync(copyRequest).GetAwaiter().GetResult();
                    DeleteS3File(S3Region, existingS3Bucket, existingS3FilePathWithFileName);

                }
                Log.Information("TransferOrRenameS3File Completed");
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
            }
        }
        /// <summary>
        /// DeleteS3File
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="S3Bucket"></param>
        /// <param name="file"></param>
        public void DeleteS3File(string S3Region, string S3Bucket, string file)
        {
            try
            {
                Log.Information("DeleteS3File Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {
                    var deleteRequest = new DeleteObjectRequest
                    {
                        BucketName = S3Bucket,
                        Key = file
                    };

                    // Execute the delete request
                    var delrequest = s3client.DeleteObjectAsync(deleteRequest).GetAwaiter().GetResult();
                }
                Log.Information("DeleteS3File Completed");
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
            }

        }
        /// <summary>
        /// ListAllFilesInS3Bucket
        /// </summary>
        /// <param name="S3Region"></param>
        /// <param name="S3Bucket"></param>
        /// <param name="S3Prefix"></param>
        /// <returns></returns>
        public List<S3Object> ListAllFilesInS3Bucket(string S3Region, string S3Bucket, string S3Prefix)
        {
            try
            {
                Log.Information("ListAllFilesInS3Bucket Starts");
                using (var s3client = new AmazonS3Client(RegionEndpoint.GetBySystemName(S3Region)))
                {
                    var request = new ListObjectsRequest
                    {
                        BucketName = S3Bucket,
                        Prefix = S3Prefix,
                        Delimiter = "/"
                    };

                    var response = s3client.ListObjectsAsync(request).GetAwaiter().GetResult();
                    Log.Information("ListAllFilesInS3Bucket Completed");
                    return response.S3Objects;

                }

            }
            catch (Exception ex)
            {
                Log.Error(ex.Message.ToString() + "\n" + ex.InnerException);
                return null;
            }
        }

    }
}
