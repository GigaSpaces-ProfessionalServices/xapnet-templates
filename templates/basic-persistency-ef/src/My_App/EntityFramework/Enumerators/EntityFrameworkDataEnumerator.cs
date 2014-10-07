using GigaSpaces.Core.Persistency;
using System;
using System.Collections;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Text;

namespace My_App.EntityFramework.Enumerators
{
    public class EntityFrameworkDataEnumerator : IDataEnumerator
    {
        private readonly DbContext _dbContext;
        private readonly Type _entityType;
        private readonly int _enumeratorLoadFetchSize;
        private readonly Query _gigaspaceQuery;
        private readonly bool _performOrderById;

        private IQueryable _currentIQueryable;
        private IEnumerator _currentEnumerator;

        private int _offset;
        private int _chunkSize;
        private int _totalEnumeratedObject;
        private int _currentFetchPosition;
        private bool _isExhausted;
        private object _current;

        public EntityFrameworkDataEnumerator(Type entityType, Query gigaspaceQuery, DbContext dbContext, int enumeratorLoadFetchSize,
            bool performOrderById, int offset, int chunkSize)
        {
            _entityType = entityType;
            _gigaspaceQuery = gigaspaceQuery;

            // Create DbContext instance per enumerator because it's not thread safe.
            // DbContext must have constructor which recieves connection string
            _dbContext = (DbContext)Activator.CreateInstance(dbContext.GetType(), dbContext.Database.Connection.ConnectionString);
            _dbContext.Configuration.LazyLoadingEnabled = false;
            _dbContext.Configuration.ProxyCreationEnabled = false;
            _enumeratorLoadFetchSize = enumeratorLoadFetchSize;
            _performOrderById = performOrderById;
            _offset = offset;
            _chunkSize = chunkSize;
            InitQuery();
        }

        public EntityFrameworkDataEnumerator(Type entityType, Query gigaspaceQuery, DbContext dbContext, int enumeratorLoadFetchSize,
            bool performOrderById)
            : this(entityType, gigaspaceQuery, dbContext, enumeratorLoadFetchSize, performOrderById, 0, int.MaxValue)
        {
        }

        public EntityFrameworkDataEnumerator(Type entityType, DbContext dbContext, int enumeratorLoadFetchSize,
            bool performOrderById)
            : this(entityType, null, dbContext, enumeratorLoadFetchSize, performOrderById, 0, int.MaxValue)
        {
        }

        public EntityFrameworkDataEnumerator(Type entityType, DbContext dbContext, int enumeratorLoadFetchSize,
            bool performOrderById, int offset, int chunkSize)
            : this(entityType, null, dbContext, enumeratorLoadFetchSize, performOrderById, offset, chunkSize)
        {
        }

        /// <summary>
        ///     Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        ///     true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of
        ///     the collection.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception>
        /// <filterpriority>2</filterpriority>
        public bool MoveNext()
        {
            //If enumerated over chunk size of objects set the state of this enumerator as exhausted
            if (_totalEnumeratedObject == _chunkSize)
                _isExhausted = true;
            if (_isExhausted)
                return false;
            if (_currentEnumerator == null || !_currentEnumerator.MoveNext())
            {
                int firstResultPosition = _offset + _currentFetchPosition * _enumeratorLoadFetchSize;
                int maxResultSize = Math.Min(_enumeratorLoadFetchSize, _chunkSize);
                if (_currentIQueryable != null)
                {
                    var _batchQueryable = _currentIQueryable.Skip(firstResultPosition).Take(maxResultSize).AsNoTracking();
                    _currentEnumerator = _batchQueryable.GetEnumerator();
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

        /// <summary>
        ///     Sets the enumerator to its initial position, which is before the first element in the collection.
        /// </summary>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception>
        /// <filterpriority>2</filterpriority>
        public void Reset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///     Gets the current element in the collection.
        /// </summary>
        /// <returns>
        ///     The current element in the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public object Current
        {
            get { return _current; }
        }

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            _dbContext.Dispose();
        }

        private void InitQuery()
        {
            try
            {
                //Handle the case of entity type (InitialLoad)
                if (_entityType != null && _gigaspaceQuery == null)
                {
                    //If perform by id, add order to the criteria
                    var query = _dbContext.Set(_entityType).AsNoTracking().AsQueryable();
                    _currentIQueryable = SetOrderBy(query); // query;
                }
                //Handle the case of Persistency.Query (GetEnumerator)
                else if (_gigaspaceQuery != null)
                {
                    string select = _gigaspaceQuery.SqlQuery;
                    object[] preparedValues = _gigaspaceQuery.Parameters;
                    //DbSqlQuery dbSqlQuery = null;
                    DbRawSqlQuery dbSqlQuery = null;
                    if (preparedValues != null)
                        dbSqlQuery = _dbContext.Database.SqlQuery(_entityType, select, preparedValues);
                        //dbSqlQuery = _dbContext.Set(_entityType).SqlQuery(select, preparedValues);
                    else
                        dbSqlQuery = _dbContext.Database.SqlQuery(_entityType, select);
                        //dbSqlQuery = _dbContext.Set(_entityType).SqlQuery(select);
                    var query = dbSqlQuery.Cast(_entityType).AsQueryable();//Cast(_entityType).AsQueryable();
                    _currentIQueryable = SetOrderBy(query);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error while constructing an enumerator", ex);
            }
        }

        private IQueryable SetOrderBy(IQueryable query)
        {
            if (_performOrderById)
            {
                var typePrimaryKeys = _dbContext.GetEntityPrimaryKeys(_entityType);
                if (typePrimaryKeys != null && typePrimaryKeys.Count() > 0)
                    query = query.OrderBy(typePrimaryKeys.First());
                else
                    query = SetOrderingWithoutPrimaryKey(query);
            }
            else
                query = SetOrderingWithoutPrimaryKey(query);
            return query;
        }

        private IQueryable SetOrderingWithoutPrimaryKey(IQueryable query)
        {
            var typeProperties = _dbContext.GetEntityMembers(_entityType);
            if (typeProperties != null && typeProperties.Count() > 0)
                query = query.OrderBy(typeProperties.First());
            return query;
        }
    }
}
