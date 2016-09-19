using System.Threading.Tasks;

namespace SnowMaker
{
    public interface IOptimisticDataStore
    {
        Task<string> GetData(string blockName);
        Task<bool> TryOptimisticWrite(string blockName, string data);
    }
}
