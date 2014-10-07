using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace My_App.EntityFramework.Enumerators.TaskEnumerators
{
    /// <summary>
    /// A linked list that can link with another linkable linked list in O(1) operations.
    /// </summary>
    internal class LinkableLinkedList
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
}
