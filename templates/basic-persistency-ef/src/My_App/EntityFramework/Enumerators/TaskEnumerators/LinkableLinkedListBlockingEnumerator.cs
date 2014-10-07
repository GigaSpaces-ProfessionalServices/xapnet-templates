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
    /// A blocking enumerator over a LinkableLinkedList, MoveNext will block until there's a next element
    /// in the list or the construction of the list is done
    /// </summary>
    internal class LinkableLinkedListBlockingEnumerator : IEnumerator
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
            if (currentQueuePosition == sharedState.currentEnumeratedObjectListSize)
            {
                lock (sharedState.syncObject)
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
}
