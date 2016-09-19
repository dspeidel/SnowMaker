using System.Threading.Tasks;

namespace SnowMaker
{
    public interface IUniqueIdGenerator
    {
        Task<long> NextId(string scopeName);
    }
}