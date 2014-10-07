using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace My_App.EntityFramework.Enumerators.TaskEnumerators
{
    /// <summary>
    /// A class that represent a shared state between all the enumerate tasks, all the tasks will share the same instance
    /// of this class
    /// </summary>
    internal class EnumerateTaskSharedState
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
}
