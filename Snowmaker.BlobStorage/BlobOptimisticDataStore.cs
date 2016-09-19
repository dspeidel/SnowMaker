using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using SnowMaker;

namespace Snowmaker.BlobStorage
{
    public class BlobOptimisticDataStore : IOptimisticDataStore
    {
        const string SeedValue = "1";

        readonly CloudBlobContainer blobContainer;

	    private readonly ConcurrentDictionary<string, ICloudBlob> blobReferences; 
        readonly object blobReferencesLock = new object();
		

		public BlobOptimisticDataStore(CloudStorageAccount account, string containerName)
        {
            var blobClient = account.CreateCloudBlobClient();
            blobContainer = blobClient.GetContainerReference(containerName.ToLower());
            blobContainer.CreateIfNotExists();

            blobReferences = new ConcurrentDictionary<string, ICloudBlob>();
        }

        public async Task<string> GetData(string blockName)
        {
            var blobReference = await GetBlobReference(blockName);
            using (var stream = new MemoryStream())
            {
                await blobReference.DownloadToStreamAsync(stream);
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        public async Task<bool> TryOptimisticWrite(string scopeName, string data)
        {
            var blobReference = await GetBlobReference(scopeName);
            try
            {
                await UploadText(
                    blobReference,
                    data,
                    AccessCondition.GenerateIfMatchCondition(blobReference.Properties.ETag));
            }
            catch (StorageException exc)
            {
                if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    return false;

                throw;
            }
            return true;
        }

	    async Task<ICloudBlob> GetBlobReference(string blockName)
        {
	        return blobReferences.GetOrAdd(blockName, await InitializeBlobReference(blockName));
        }

        private async Task<ICloudBlob> InitializeBlobReference(string blockName)
        {
            var blobReference = blobContainer.GetBlockBlobReference(blockName);

            if (blobReference.Exists())
                return blobReference;

            try
            {
                await UploadText(blobReference, SeedValue, AccessCondition.GenerateIfNoneMatchCondition("*"));
            }
            catch (StorageException uploadException)
            {
                if (uploadException.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
            }

            return blobReference;
        }

        async Task UploadText(ICloudBlob blob, string text, AccessCondition accessCondition)
        {
            blob.Properties.ContentEncoding = "UTF-8";
            blob.Properties.ContentType = "text/plain";
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(text)))
            {
	            //TODO: why are these extra objects necessary?
				await blob.UploadFromStreamAsync(stream, accessCondition, new BlobRequestOptions(), new OperationContext());
            }
        }
    }
}
