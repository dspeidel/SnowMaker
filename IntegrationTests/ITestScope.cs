using System;

namespace IntegrationTests.cs
{
    public interface ITestScope : IDisposable
    {
        string IdScopeName { get; }
        string ReadCurrentPersistedValue();
	    string ReadCurrentPersistedValue(string scopeId);

    }
}