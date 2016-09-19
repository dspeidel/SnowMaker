using System.IO;
using System.Threading.Tasks;

namespace SnowMaker
{
    public class DebugOnlyFileDataStore : IOptimisticDataStore
    {
        const string SeedValue = "1";
		private readonly object locker = new object();
        readonly string directoryPath;

        public DebugOnlyFileDataStore(string directoryPath)
        {
            this.directoryPath = directoryPath;
        }

        public Task<string> GetData(string blockName)
        {
            var blockPath = Path.Combine(directoryPath, string.Format("{0}.txt", blockName));
            try
            {
                return Task.FromResult(File.ReadAllText(blockPath));
            }
            catch (FileNotFoundException)
            {
                using (var file = File.Create(blockPath))
                using (var streamWriter = new StreamWriter(file))
                {
                    streamWriter.Write(SeedValue);
                }
                return Task.FromResult(SeedValue);
            }
        }

        public Task<bool> TryOptimisticWrite(string blockName, string data)
        {
	        lock (locker)
	        {
		        var blockPath = Path.Combine(directoryPath, string.Format("{0}.txt", blockName));
		        File.WriteAllText(blockPath, data);
		        return Task.FromResult(true);
	        }
        }
    }
}
