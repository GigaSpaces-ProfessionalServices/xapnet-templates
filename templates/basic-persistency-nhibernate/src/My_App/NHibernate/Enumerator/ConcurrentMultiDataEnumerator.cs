using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using GigaSpaces.Core.Persistency;

namespace My_App.NHibernate.Enumerator
{
    /// <summary>
    /// A Concurrent implementation of IDataEnumerator that receives a list of DataEnumerators and enumerate over them
    /// concurrently while exposing them as one DataEnumerator
    /// </summary>
    public class ConcurrentMultiDataEnumerator : IDataEnumerator
    {
        #region Members
        private readonly ICollection<IDataEnumerator> _internalDataEnumerators;
        private readonly int _batchSize;        
        private readonly int _threadPoolSize;

        private IEnumerator _internalEnumerator;        
        private bool _startedPolling;
        private readonly EnumerateTaskSharedState _sharedState;
    	private volatile Thread[] _threadPool;		
        #endregion

        #region Constructors
		public ConcurrentMultiDataEnumerator(ICollection<IDataEnumerator> internalDataEnumerators, int batchSize, int threadPoolSize)
        {
            _batchSize = batchSize;            
            _threadPoolSize = threadPoolSize;
            _internalDataEnumerators = internalDataEnumerators;

            _sharedState = new EnumerateTaskSharedState(_internalDataEnumerators.Count);
        }

        #endregion

        #region Private methods
        /// <summary>
        /// Starts polling objects from the enumerators and fill them within the objects queue
        /// </summary>
        private void StartPollingObjects()
        {
            Queue<EnumerateTask> enumerateTasksQueue = new Queue<EnumerateTask>();
            if (_internalDataEnumerators.Count == 0)
                _sharedState.allEnumeratorsDone = true;
            //Insert enumerate tasks into the task queue for each enumerator
	        foreach (IDataEnumerator enumerator in _internalDataEnumerators)
				enumerateTasksQueue.Enqueue(new EnumerateTask(_sharedState, _batchSize, enumerator));
			//Creates a pool of threads that will take tasks from the queue and execute it
            int actualThreadPoolSize = Math.Min(_threadPoolSize, _internalDataEnumerators.Count);
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
        
        #endregion

        #region IDataEnumerator implementation

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
            throw new NotSupportedException();
        }

        public object Current
        {
            get { return _internalEnumerator.Current; }
        }

        public void Dispose()
        {
			//Signal concurrent tasks that the enumerator is disposed and they should stop working
			lock (_sharedState.syncObject)
			{
				_sharedState.disposed = true;
				Monitor.PulseAll(_sharedState.syncObject);
			}
			for (int i = 0; i < _threadPool.Length; ++i)
				_threadPool[i].Join();
        	//Dispose all the internal enumerators
            foreach(IDataEnumerator enumerator in _internalDataEnumerators)
                enumerator.Dispose();
        }

        #endregion

        #region Nested classes
        /// <summary>
        /// A class that represent a shared state between all the enumerate tasks, all the tasks will share the same instance
        /// of this class
        /// </summary>
        private class EnumerateTaskSharedState
        {
            public readonly object syncObject;
            public readonly LinkableLinkedList accumulatedEnumeratedObjectList;
            public readonly int numberOfEnumerateTask;
            public int enumeratorsDoneCount;
            public volatile bool allEnumeratorsDone;
            public Exception exceptionOccured;
            public int currentEnumeratedObjectListSize;
        	public volatile bool disposed;

        	public EnumerateTaskSharedState(int numberOfEnumerateTask)
            {
                this.numberOfEnumerateTask = numberOfEnumerateTask;
                syncObject = new object();
                accumulatedEnumeratedObjectList = new LinkableLinkedList();                
            }
        }
        /// <summary>
        /// Repesent an enumeration task that needs to be executed
        /// </summary>
        private class EnumerateTask
        {
            private readonly IEnumerator enumerator;
            private readonly int batchSize;
            private readonly EnumerateTaskSharedState sharedState;

            public EnumerateTask(EnumerateTaskSharedState sharedState, int batchSize, IEnumerator enumerator)
            {
                this.sharedState = sharedState;
                this.enumerator = enumerator;
                this.batchSize = batchSize;
            }

            /// <summary>
            /// Executes the enumeration task
            /// </summary>
            public void ExecuteTask()
            {
                try
                {
                    //Create a local link list for the current task
                    LinkableLinkedList batch = new LinkableLinkedList();
                    int batchPosition = 0;
                    while (!sharedState.disposed && enumerator.MoveNext())
                    {
                        batch.AppendValue(enumerator.Current);
                        //If the position within the enumerator reached batch size add it to the accumulated linked list
                        if (++batchPosition == batchSize)
                        {
                            lock (sharedState.syncObject)
                            {
                                AppendListToAccumulatedList(batch);
                                //Reset batch
                                batchPosition = 0;
                                batch = new LinkableLinkedList();
                            }
                        }
                    }
                    //Add last batch if needed
                    if (batchPosition > 0)
                    {
                        AppendListToAccumulatedList(batch);
                    }
                    //If this is the last task, set state if sharedState to done
                    if (Interlocked.Increment(ref sharedState.enumeratorsDoneCount) == sharedState.numberOfEnumerateTask)
                    {
                        lock (sharedState.syncObject)
                        {
                            sharedState.allEnumeratorsDone = true;
                            Monitor.PulseAll(sharedState.syncObject);
                        }
                    }
                }
                catch(Exception ex)
                {
                    //On any exception update the shared state
                    lock (sharedState.syncObject)
                    {
                        sharedState.exceptionOccured = ex;
                        Monitor.PulseAll(sharedState.syncObject);
                    }
                }

            }

            /// <summary>
            /// Add given list to the accumulated list
            /// </summary>
            /// <param name="list"></param>
            private void AppendListToAccumulatedList(LinkableLinkedList list)
            {
                lock (sharedState.syncObject)
                {
                    sharedState.accumulatedEnumeratedObjectList.AppentList(list);
                    sharedState.currentEnumeratedObjectListSize = sharedState.accumulatedEnumeratedObjectList.Count;
                    Monitor.PulseAll(sharedState.syncObject);
                }
            }
        }

        /// <summary>
        /// A blocking enumerator over a LinkableLinkedList, MoveNext will block until there's a next element
        /// in the list or the construction of the list is done
        /// </summary>
        private class LinkableLinkedListBlockingEnumerator : IEnumerator
        {
            private readonly EnumerateTaskSharedState sharedState;            
            private int currentQueuePosition;
            private LinkableLinkedListNode currentPositionNode;

            public LinkableLinkedListBlockingEnumerator(EnumerateTaskSharedState sharedState)
            {
                this.sharedState = sharedState;                

                while (currentPositionNode == null)
                    lock (sharedState.syncObject)
                    {
                        //Check for any exception thrown by the task executers
                        if (sharedState.exceptionOccured != null)
                            throw sharedState.exceptionOccured;
                        //Create first node when the accumulated list has elements
                        if (sharedState.accumulatedEnumeratedObjectList.First != null)
                        {
                            currentPositionNode = new LinkableLinkedListNode(null);
                            currentPositionNode.Next = sharedState.accumulatedEnumeratedObjectList.First;
                        }
                        //If all enumerators are done or enumerator is disposed, break
                        else if (sharedState.allEnumeratorsDone || sharedState.disposed)
                            break;
                        //If there are no elements in the list yet block until status changed
                        else
                            Monitor.Wait(sharedState.syncObject);                        
                    }
            }

            public bool MoveNext()
            {
                if(currentQueuePosition == sharedState.currentEnumeratedObjectListSize)
                {                    
                    lock(sharedState.syncObject)
                    {   //double check instead the size was changed between the while and the lock
                        while (currentQueuePosition == sharedState.currentEnumeratedObjectListSize)
                        {
                            //Check for any exception thrown by the task executers
                            if (sharedState.exceptionOccured != null)
                                throw sharedState.exceptionOccured;
                            //If all enumerators are done or enumerator is disposed return false                            
                            if (sharedState.allEnumeratorsDone || sharedState.disposed)
                                return false;
                            //If not done creating the list block until the list status has changed
                            else
                                Monitor.Wait(sharedState.syncObject);
                        }
                    }
                }
                
                currentPositionNode = currentPositionNode.Next;
                currentQueuePosition++;
                
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public object Current
            {
                get { return currentPositionNode.Value; }
            }
        }
        #region LinkableLinkedList class
        /// <summary>
        /// A linked list that can link with another linkable linked list in O(1) operations.
        /// </summary>
        private class LinkableLinkedList
        {
            private LinkableLinkedListNode _first;
            private LinkableLinkedListNode _last;
            private int _count;

            public LinkableLinkedListNode First
            {
                get { return _first; }
            }

            public LinkableLinkedListNode Last
            {
                get { return _last; }
            }

            public int Count
            {
                get { return _count; }
            }

            public void AppendValue(object value)
            {
                //Empty list
                if (_first == null)
                {
                    _first = new LinkableLinkedListNode(value);
                    _last = _first;
                    _count = 1;
                }
                else
                {
                    _last.Next = new LinkableLinkedListNode(value);
                    _last = _last.Next;
                    _count++;
                }
            }

            public void AppentList(LinkableLinkedList list)
            {
                //Empty list
                if (_first == null)
                {
                    _first = list.First;
                    _last = list.Last;
                    _count = list.Count;
                }
                else
                {
                    _last.Next = list.First;
                    //If the given list to append is empty
                    if (list.Last != null)
                        _last = list.Last;
                    _count += list.Count;
                }
            }
        }

        /// <summary>
        /// A node inside a LinkableLinkedList
        /// </summary>
        private class LinkableLinkedListNode
        {
            private readonly object _value;
            private LinkableLinkedListNode _next;

            public LinkableLinkedListNode(object value)
            {
                _value = value;
            }

            public object Value
            {
                get { return _value; }
            }

            public LinkableLinkedListNode Next
            {
                get { return _next; }
                set { _next = value; }
            }
        }
        #endregion

        #endregion

    }

}
