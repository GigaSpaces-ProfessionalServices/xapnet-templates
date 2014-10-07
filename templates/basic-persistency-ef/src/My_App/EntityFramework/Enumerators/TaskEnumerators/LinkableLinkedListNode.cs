using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace My_App.EntityFramework.Enumerators.TaskEnumerators
{
    /// <summary>
    /// A node inside a LinkableLinkedList
    /// </summary>
    internal class LinkableLinkedListNode
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
}
