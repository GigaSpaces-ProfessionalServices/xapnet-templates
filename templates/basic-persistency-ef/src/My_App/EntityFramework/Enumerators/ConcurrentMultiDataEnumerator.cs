using System.Collections.Generic;
using GigaSpaces.Core.Persistency;
using System;
using System.Threading;
using System.Collections;
using My_App.EntityFramework.Enumerators.TaskEnumerators;

namespace My_App.EntityFramework.Enumerators
{
    public class ConcurrentMultiDataEnumerator : IDataEnumerator
    {
        private bool _startedPolling;
        private int _threadPoolSize;
        private int _enumeratorLoadFetchSize;
        private IList<IDataEnumerator> _enumerators;
        private readonly EnumerateTaskSharedState _sharedState;
    	private volatile Thread[] _threadPool;		
        private IEnumerator _internalEnumerator;

        public ConcurrentMultiDataEnumerator(IList<IDataEnumerator> enumerators, int enumeratorLoadFetchSize, int threadPoolSize)
        {
            _enumerators = enumerators;
            _threadPoolSize = threadPoolSize;
            _enumeratorLoadFetchSize = enumeratorLoadFetchSize;
            _sharedState = new EnumerateTaskSharedState(enumerators.Count);
        }

         /// <summary>
        /// Starts polling objects from the enumerators and fill them within the objects queue
        /// </summary>
        private void StartPollingObjects()
        {
            Queue<EnumerateTask> enumerateTasksQueue = new Queue<EnumerateTask>();
            if (_enumerators.Count == 0)
                _sharedState.allEnumeratorsDone = true;
            //Insert enumerate tasks into the task queue for each enumerator
	        foreach (IDataEnumerator enumerator in _enumerators)
				enumerateTasksQueue.Enqueue(new EnumerateTask(_sharedState, _enumeratorLoadFetchSize, enumerator));
			//Creates a pool of threads that will take tasks from the queue and execute it
            int actualThreadPoolSize = Math.Min(_threadPoolSize, _enumerators.Count);
            _threadPool = new Thread[actualThreadPoolSize];            
            for (int i = 0; i < actualThreadPoolSize; i++)
            {
                ThreadStart threadStart = delegate
                                              {
                                                  try
                                                  {
                                                      while (!_sharedState.disposed)
                                                      {
                                                          EnumerateTask task;
                                                          lock (enumerateTasksQueue)
                                                          {
                                                              //Take task
                                                              task = enumerateTasksQueue.Dequeue();
                                                          }
                                                          //Execute task if such exists
                                                          if (task != null)
                                                              task.ExecuteTask();
                                                      }
                                                  }
                                                  catch (InvalidOperationException)
                                                  {
                                                      //Terminate thread, no more tasks in queue
                                                  }
                                              };
				_threadPool[i] = new Thread(threadStart);
            }
            //Starts the thread pool
            for (int i = 0; i < actualThreadPoolSize; i++)
            {
				_threadPool[i].Start();
            }
            //Starts the internal enumerator
            _internalEnumerator = new LinkableLinkedListBlockingEnumerator(_sharedState);
        }        
        
        public bool MoveNext()
        {
            if (!_startedPolling)
            {
                StartPollingObjects();
                _startedPolling = true;
            }
            return _internalEnumerator.MoveNext();
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public object Current 
        {
            get { return _internalEnumerator.Current; }
        }

        public void Dispose()
        {
            lock (_sharedState.syncObject)
            {
                _sharedState.disposed = true;
                Monitor.PulseAll(_sharedState.syncObject);
            }
            for (int i = 0; i < _threadPool.Length; ++i)
                _threadPool[i].Join();
            foreach (IDataEnumerator enumerator in _enumerators)
                enumerator.Dispose();
        }
    }
}