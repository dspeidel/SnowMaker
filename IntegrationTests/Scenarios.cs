using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using SnowMaker;

namespace IntegrationTests.cs
{
	public abstract class Scenarios<TTestScope> where TTestScope : ITestScope
	{
		protected abstract IOptimisticDataStore BuildStore(TTestScope scope);
		protected abstract TTestScope BuildTestScope();

		[Test]
		public async Task ShouldReturnOneForFirstIdInNewScope()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store = BuildStore(testScope);
				var generator = new UniqueIdGenerator(store) { BatchSize = 3 };

				// Act
				var generatedId = await generator.NextId(testScope.IdScopeName);

				// Assert
				Assert.AreEqual(1, generatedId);
			}
		}

		[Test]
		public async Task ShouldInitializeBlobForFirstIdInNewScope()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store = BuildStore(testScope);
				var generator = new UniqueIdGenerator(store) { BatchSize = 3 };

				// Act
				await generator.NextId(testScope.IdScopeName); //1

				// Assert
				Assert.AreEqual("4", testScope.ReadCurrentPersistedValue());
			}
		}

		[Test]
		public async Task ShouldNotUpdateBlobAtEndOfBatch()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store = BuildStore(testScope);
				var generator = new UniqueIdGenerator(store) { BatchSize = 3 };

				// Act
				await generator.NextId(testScope.IdScopeName); //1
				await generator.NextId(testScope.IdScopeName); //2
				await generator.NextId(testScope.IdScopeName); //3

				// Assert
				Assert.AreEqual("4", testScope.ReadCurrentPersistedValue());
			}
		}

		[Test]
		public async Task ShouldUpdateBlobWhenGeneratingNextIdAfterEndOfBatch()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store = BuildStore(testScope);
				var generator = new UniqueIdGenerator(store) { BatchSize = 3 };

				// Act
				await generator.NextId(testScope.IdScopeName); //1
				await generator.NextId(testScope.IdScopeName); //2
				await generator.NextId(testScope.IdScopeName); //3
				await generator.NextId(testScope.IdScopeName); //4

				// Assert
				Assert.AreEqual("7", testScope.ReadCurrentPersistedValue());
			}
		}

		[Test]
		public async Task ShouldReturnIdsFromThirdBatchIfSecondBatchTakenByAnotherGenerator()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store1 = BuildStore(testScope);
				var generator1 = new UniqueIdGenerator(store1) { BatchSize = 3 };
				var store2 = BuildStore(testScope);
				var generator2 = new UniqueIdGenerator(store2) { BatchSize = 3 };

				// Act
				await generator1.NextId(testScope.IdScopeName); //1
				await generator1.NextId(testScope.IdScopeName); //2
				await generator1.NextId(testScope.IdScopeName); //3
				await generator2.NextId(testScope.IdScopeName); //4
				var lastId = await generator1.NextId(testScope.IdScopeName); //7

				// Assert
				Assert.AreEqual(7, lastId);
			}
		}

		[Test]
		public async Task ShouldReturnIdsAcrossMultipleGenerators()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store1 = BuildStore(testScope);
				var generator1 = new UniqueIdGenerator(store1) { BatchSize = 3 };
				var store2 = BuildStore(testScope);
				var generator2 = new UniqueIdGenerator(store2) { BatchSize = 3 };

				// Act
				var generatedIds = new[]
				{
					await generator1.NextId(testScope.IdScopeName), //1
                    await generator1.NextId(testScope.IdScopeName), //2
                    await generator1.NextId(testScope.IdScopeName), //3
                    await generator2.NextId(testScope.IdScopeName), //4
                    await generator1.NextId(testScope.IdScopeName), //7
                    await generator2.NextId(testScope.IdScopeName), //5
                    await generator2.NextId(testScope.IdScopeName), //6
                    await generator2.NextId(testScope.IdScopeName), //10
                    await generator1.NextId(testScope.IdScopeName), //8
                    await generator1.NextId(testScope.IdScopeName)  //9
                };

				// Assert
				CollectionAssert.AreEqual(
					new[] { 1, 2, 3, 4, 7, 5, 6, 10, 8, 9 },
					generatedIds);
			}
		}

		[Test]
		public async Task ShouldReturnIdsAcrossMultipleGenerators2()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store1 = BuildStore(testScope);
				var generator1 = new UniqueIdGenerator(store1) { BatchSize = 1 };
				var store2 = BuildStore(testScope);
				var generator2 = new UniqueIdGenerator(store2) { BatchSize = 1 };

				// Act
				var generatedIds = new[]
				{
					await generator1.NextId(testScope.IdScopeName), //1
                    await generator1.NextId(testScope.IdScopeName), //2
                    await generator1.NextId(testScope.IdScopeName), //3
                    await generator2.NextId(testScope.IdScopeName), //4
                    await generator1.NextId(testScope.IdScopeName), //5
                    await generator2.NextId(testScope.IdScopeName), //6
                    await generator2.NextId(testScope.IdScopeName), //7
                    await generator2.NextId(testScope.IdScopeName), //8
                    await generator1.NextId(testScope.IdScopeName), //9
                    await generator1.NextId(testScope.IdScopeName)  //10
                };

				// Assert
				CollectionAssert.AreEqual(
					new[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, generatedIds);
			}
		}

		[Test]
		public void ShouldSupportUsingOneGeneratorFromMultipleThreads()
		{
			// Arrange
			using (var testScope = BuildTestScope())
			{
				var store = BuildStore(testScope);
				var generator = new UniqueIdGenerator(store) { BatchSize = 1000 };
				const int testLength = 10000;

				// Act
				var generatedIds = new ConcurrentQueue<long>();
				var threadIds = new ConcurrentQueue<int>();
				var scopeName = testScope.IdScopeName;
				Parallel.For(
					0,
					testLength,
					new ParallelOptions { MaxDegreeOfParallelism = 10 },
					async i =>
					{
						generatedIds.Enqueue(await generator.NextId(scopeName));
						threadIds.Enqueue(Thread.CurrentThread.ManagedThreadId);
					});

				// Assert we generated the right count of ids
				Assert.AreEqual(testLength, generatedIds.Count);

				// Assert there were no duplicates
				Assert.IsFalse(generatedIds.GroupBy(n => n).Any(g => g.Count() != 1));

				// Assert we used multiple threads
				var uniqueThreadsUsed = threadIds.Distinct().Count();
				if (uniqueThreadsUsed == 1)
					Assert.Inconclusive("The test failed to actually utilize multiple threads");
			}
		}
	}
}
