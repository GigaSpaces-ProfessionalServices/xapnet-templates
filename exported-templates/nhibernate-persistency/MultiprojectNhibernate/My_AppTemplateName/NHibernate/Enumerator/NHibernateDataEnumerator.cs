using System;
using System.Collections;
using GigaSpaces.Core.Persistency;
using NHibernate;
using NHibernate.Criterion;
using NHibernate.Metadata;
using NHibernate.Proxy;

namespace $saferootprojectname$.NHibernate.Enumerator
{
    /// <summary>
    /// Implementation of IDataEnumerators over NHibernate
    /// </summary>
    public class NHibernateDataEnumerator : IDataEnumerator
    {
        
        #region Members
        private readonly string _entityType;
        private readonly ISessionFactory _sessionFactory;
        private readonly int _fetchSize;
        private readonly Query _persistencyQuery;
        private readonly bool _performOrderById;
        private readonly int _offset;
        private readonly int _chunkSize;
        private ISession _session;
        private ITransaction _tx;
        private IQuery _query;
        private ICriteria _criteria;
        private IEnumerator _currentEnumerator;

        private int _currentFetchPosition;        
        private object _current;
        private bool _isExhausted; 
        private int _totalEnumeratedObject;
        #endregion

        #region Constructors

        public NHibernateDataEnumerator(string entityType, ISessionFactory sessionFactory, int fetchSize,
                                        bool performOrderById, int offset, int chunkSize)
        {
            _entityType = entityType;
            _performOrderById = performOrderById;
            _sessionFactory = sessionFactory;
            _fetchSize = fetchSize;
            _chunkSize = chunkSize;
            _offset = offset;            
        }

        public NHibernateDataEnumerator(Query persistencyQuery, ISessionFactory sessionFactory, int fetchSize, bool performOrderById):this((string)null, sessionFactory, fetchSize, performOrderById)
        {                    
            _persistencyQuery = persistencyQuery;            
        }

        public NHibernateDataEnumerator(string entityType, ISessionFactory sessionFactory, int fetchSize, bool performOrderById):this(entityType, sessionFactory, fetchSize, performOrderById, 0, int.MaxValue)
        {            
        }

        #endregion

        #region Private methods
        /// <summary>
        /// Initializes the session that will be used to enumerate
        /// </summary>
        private void InitSession()
        {
            _session = _sessionFactory.OpenSession();
            _session.FlushMode = FlushMode.Never;
            _tx = _session.BeginTransaction();
            try
            {
                //Handle the case of entity type (InitialLoad)
                if (_entityType != null)
                {
                    _criteria = _session.CreateCriteria(_entityType);
                    _criteria.SetCacheable(false);
                    //If perform by id, add order to the criteria
                    if (_performOrderById)
                    {
                        IClassMetadata metadata = _sessionFactory.GetClassMetadata(_entityType);
                        string idPropName = metadata.IdentifierPropertyName;
                        if (idPropName != null)
                            _criteria.AddOrder(Order.Asc(idPropName));
                    }
                }
                //Handle the case of Persistency.Query (GetEnumerator)
                else if (_persistencyQuery != null)
                {
                    string select = _persistencyQuery.SqlQuery;
                    _query = _session.CreateQuery(select);
                    object[] preparedValues = _persistencyQuery.Parameters;
                    if (preparedValues != null)
                    {
                        for (int i = 0; i < preparedValues.Length; i++)
                        {
                            _query.SetParameter(i, preparedValues[i]);
                        }
                    }
                    _query.SetCacheable(false);
                    _query.SetFlushMode(FlushMode.Never);                    
                }
                else throw new Exception("NHibernateDataEnumerator must receive an Entity Type or a Query");
            }
            catch(Exception ex)
            {
                if (_tx != null && _tx.IsActive)
                    _tx.Rollback();
                if (_session.IsOpen)
                    _session.Close();
                throw new Exception("Error while constructing an enumerator", ex);
            }
        }
        /// <summary>
        /// Gets the next element from the enumerator and returns true if such exists
        /// </summary>
        /// <returns></returns>
        private bool AssignNextElement()
        {
            if (_session == null)
                InitSession();
            //If enumerated over chunk size of objects set the state of this enumerator as exhausted
            if (_totalEnumeratedObject == _chunkSize)
                _isExhausted = true;
            if (_isExhausted)
            {
                return false;
            }
            if (_currentEnumerator == null || !_currentEnumerator.MoveNext())
            {
                int firstResultPosition = _offset + _currentFetchPosition * _fetchSize;                
                int maxResultSize = Math.Min(_fetchSize, _chunkSize);
                if (_query != null)
                {
                    _query.SetFirstResult(firstResultPosition);
                    _query.SetMaxResults(maxResultSize);
                    _currentEnumerator = _query.List().GetEnumerator();
                    _currentFetchPosition++;
                }
                else
                { // criteria != null
                    _criteria.SetFirstResult(firstResultPosition);
                    _criteria.SetMaxResults(maxResultSize);
                    _currentEnumerator = _criteria.List().GetEnumerator();
                    _currentFetchPosition++;
                }
                if (!_currentEnumerator.MoveNext())
                {
                    _current = null;
                    _isExhausted = true;
                    return false;
                }
            }
            _totalEnumeratedObject++;
            _current = _currentEnumerator.Current;            
            return true;
        }        
        #endregion

        #region IDisposable Members

        public void Dispose()
        {            
            if (_session != null && _session.IsOpen)
            {
                try
                {
                    if (_tx != null && _tx.IsActive)
                        _tx.Commit();
                }
                finally
                {
                    _session.Close();
                }
            }
        }

        #endregion

        #region IEnumerator Members        
        public bool MoveNext()
        {
            return AssignNextElement();		                			
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public object Current
        {
            get
            {
                if (_current != null && _current is INHibernateProxy)
                {
                    INHibernateProxy proxy = _current as INHibernateProxy;
                    _current = proxy.HibernateLazyInitializer.GetImplementation();
                }
                return _current;
            }
        }

        #endregion
    }
}