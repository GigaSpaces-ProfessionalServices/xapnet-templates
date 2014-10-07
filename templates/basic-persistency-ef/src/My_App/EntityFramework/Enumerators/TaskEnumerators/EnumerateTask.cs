using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace My_App.EntityFramework.Enumerators.TaskEnumerators
{
    /// <summary>
    /// Repesent an enumeration task that needs to be executed
    /// </summary>
    internal class EnumerateTask
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
            catch (Exception ex)
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
}
