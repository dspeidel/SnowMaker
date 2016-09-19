using Nito.AsyncEx;

namespace SnowMaker
{
    class ScopeState
    {
        //public readonly object IdGenerationLock = new object();
		public readonly AsyncLock IdGenerationLock = new AsyncLock();
		
		public long LastId;
        public long HighestIdAvailableInBatch;
    }
}