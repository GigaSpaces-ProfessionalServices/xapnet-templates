using GigaSpaces.Core.Persistency;
using My_App.EntityFramework.Enumerators;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;

namespace My_App.EntityFramework
{
    public class EntityFrameworkExternalDataSource : AbstractExternalDataSource
    {
        private DbContext _dbContext;
        internal Type[] _initialLoadEntries;
        private int _initialLoadChunkSize = 100000;
        private int _enumeratorLoadFetchSize = 10000;
        private bool _performOrderById = true;
        private int _initialLoadThreadPoolSize = 10;
        private EFManager _efManager;

        public EntityFrameworkExternalDataSource()
        { }

        public EntityFrameworkExternalDataSource(DbContext dbContext)
        {
            _dbContext = dbContext;
            SetOptionsForDbContext(_dbContext);
            _efManager = new EFManager(dbContext);
            _efManager.ReloadDbEntries();
        }

        public EntityFrameworkExternalDataSource(DbContext dbContext, Type[] typesToLoad)
        {
            _dbContext = dbContext;
            SetOptionsForDbContext(_dbContext);
            _initialLoadEntries = typesToLoad;
            _efManager = new EFManager(dbContext);
            _efManager.ReloadDbEntries();
        }

        internal EntityFrameworkExternalDataSource(DbContext dbContext, EFManager efManager)
        {
            _dbContext = dbContext;
            SetOptionsForDbContext(_dbContext);
            _efManager = efManager;
        }

        /// <summary>
        /// Initialize and configure the data source using given properties.
        /// Called when space is started.
        /// </summary>
        /// <param name="properties">Propeties to initialize by.</param>
        public override void Init(Dictionary<string, string> properties)
        {
            base.Init(properties);

            _enumeratorLoadFetchSize = GetIntProperty("EnumeratorLoadFetchSize", _enumeratorLoadFetchSize);
            _initialLoadChunkSize = GetIntProperty("InitialLoadChunkSize", _initialLoadChunkSize);
            _performOrderById = GetBoolProperty("PerformOrderById", _performOrderById);
            _initialLoadThreadPoolSize = GetIntProperty("InitialLoadThreadPoolSize", _initialLoadThreadPoolSize);

            if (_dbContext == null)
            {
                string dbContextTypeName = GetProperty("EntityFrameworkContextFullTypeName");
                if (string.IsNullOrWhiteSpace(dbContextTypeName))
                    throw new ArgumentException("Could not initialize DbContext instance because parameter 'EntityFrameworkContextFullTypeName' is null or whitespace.");

                string entityFrameworkConnectionString = GetProperty("EntityFrameworkConnectionString");
                if (string.IsNullOrWhiteSpace(dbContextTypeName))
                    throw new ArgumentException("Could not initialize DbContext instance because parameter 'EntityFrameworkConnectionString' is null or whitespace.");

                var type = Type.GetType(dbContextTypeName);
                if (type == null)
                    throw new ArgumentException("Could not resolve type by name. Type name must have following format: 'GigaSpaces.ProjectNamespace.StockSbaContextClassName, AssemblyName'");

                _dbContext = (DbContext)Activator.CreateInstance(type, entityFrameworkConnectionString);
                SetOptionsForDbContext(_dbContext);
                _efManager = new EFManager(_dbContext);
                _efManager.ReloadDbEntries();
            }

            if (_initialLoadEntries == null)
            {
                _dbContext.GetValidationErrors();
                // TODO: Find a way to load these without so much investigation on usage.
                _initialLoadEntries = _efManager.DbEntries;
            }
        }

        /// <summary>
        /// Creates and returns an enumerator over all the entries that should be loaded into space.
        /// </summary>
        /// <returns>
        /// Enumerator over all the entries that should be loaded into space.
        /// </returns>
        public override IDataEnumerator InitialLoad()
        {
            List<IDataEnumerator> enumerators = new List<IDataEnumerator>();
            foreach (var queryType in _initialLoadEntries)
            {
                if (InitialLoadChunkSize == -1)
                    enumerators.Add(GetEnumerator(queryType, 0, int.MaxValue));
                else
                    enumerators.AddRange(DivideToChunks(queryType));
            }
            return new ConcurrentMultiDataEnumerator(enumerators, _enumeratorLoadFetchSize, InitialLoadThreadPoolSize);
        }

        /// <summary>
        /// Close the data source and clean any used resources.
        /// Called before space shutdown.
        /// </summary>
        public override void Shutdown()
        {
            try
            {
                if (_dbContext != null)
                {
                    if (_dbContext.Database.Connection.State == System.Data.ConnectionState.Open)
                        _dbContext.Database.Connection.Close();
                    _dbContext.Dispose();
                }
            }
            catch
            {

            }
        }

        /// <summary>
        /// Create an enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query"/>.
        /// </summary>
        /// <param name="query">The Query used for matching.</param>
        /// <returns>
        /// Enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query"/>.
        /// </returns>
        public override IDataEnumerator GetEnumerator(Query query)
        {
            var entityType = _efManager.GetEntityTypeFromSqlQuery(query.SqlQuery);
            var updatedQuery = _efManager.UpdateQueryFormat(query);
            return new EntityFrameworkDataEnumerator(entityType, updatedQuery, _dbContext, _enumeratorLoadFetchSize, _performOrderById);
        }
        //Query.SqlQuery has following format
        //SqlQuery = "FROM EFEdsTest.Model.Entities.Student WHERE Id = ?"

        /// <summary>
        /// Create an enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.		            
        /// </summary>
        /// <param name="entityType">The entity type to match.</param>
        /// <param name="from">Base index to load from. If null, loads everything.</param>
        /// <param name="maxResults">Maximum results to return</param>
        /// <returns>
        /// Enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.
        /// </returns>
        public virtual IDataEnumerator GetEnumerator(Type entityType, int from, int maxResults)
        {
            return new EntityFrameworkDataEnumerator(entityType, _dbContext, _enumeratorLoadFetchSize, _performOrderById, from, maxResults);
        }

        /// <summary>
        /// Execute given bulk of operations.
        ///              Each <see cref="T:GigaSpaces.Core.Persistency.BulkItem"/> contains one of the following operation -
        ///              WRITE - given object should be inserted to the data store,
        ///              UPDATE - given object should be updated in the data store,
        ///              REMOVE - given object should be deleted from the data store
        ///              If the implementation uses transactions,
        ///              all the bulk operations must be executed in one transaction.
        /// </summary>
        /// <param name="bulk">Collection of bulk items to execute</param>
        public override void ExecuteBulk(IList<BulkItem> bulk)
        {
            using (var transaction = _dbContext.Database.BeginTransaction())
            {
                try
                {
                    foreach (var bulkItem in bulk)
                    {
                        var entity = bulkItem.Item;

                        if (entity != null)
                        {
                            var entityType = entity.GetType();
                            switch (bulkItem.Operation)
                            {
                                case BulkOperation.Remove:
                                    {
                                        var merged = _dbContext.Merge(entity);
                                        _dbContext.Set(entityType).Remove(merged);
                                        break;
                                    }
                                case BulkOperation.Update:
                                    {
                                        _dbContext.Merge(entity);
                                        break;
                                    }
                                case BulkOperation.Write:
                                    _dbContext.Set(entityType).Add(entity);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }
                        }
                    }
                    _dbContext.SaveChanges();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                }
            }
        }

        /// <summary>
        /// Gets or sets the initial load chunk size.
        /// By default, the initial load process will chunk large tables and will iterate over the table (entity) per
        /// chunk (concurrently). This setting allows to control the chunk size to split the table by. By default, set
        /// 100,000. Batching can be disabled by setting -1.     
        /// </summary>
        public int InitialLoadChunkSize
        {
            get { return _initialLoadChunkSize; }
            set
            {
                if (value < 1 && value != -1)
                    throw new ArgumentException("InitialLoadChunkSize must be a positive number or -1");
                _initialLoadChunkSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the InitialLoad operation thread pool size. The InitialLoad operation uses the <see cref="GigaSpaces.Practices.ExternalDataSource.NHibernate.Enumerator.ConcurrentMultiDataEnumerator"/>.
        /// This property allows to control the thread pool size of the concurrent multi data iterator. Defaults to
        /// 10.
        ///
        /// Note, this usually will map one to one to the number of open connections against the database.
        /// </summary>
        public int InitialLoadThreadPoolSize
        {
            get { return _initialLoadThreadPoolSize; }
            set
            {
                if (value < 1)
                    throw new ArgumentException("InitialLoadThreadPoolSize must be a positive number");
                _initialLoadThreadPoolSize = value;
            }
        }

        /// <summary>
        /// Gets or sets whether to perform InitialLoad ordered by id, this flag indicates if the generated query will order the results by
        /// the id. By default it is set to false, in some cases it might result in better initial load performance.
        /// </summary>
        public bool PerformOrderById
        {
            get { return _performOrderById; }
            set { _performOrderById = value; }
        }

        /// <summary>
        /// Gets or sets a list of entries that will be used to perform the InitialLoad operation. By default, will
        /// try and build a sensible list based on NHiberante meta data.
        ///
        /// Note, sometimes an explicit list should be provided. For example, if we have a class A and class B, and
        /// A has a relationship to B which is not component. If in the space, we only wish to have A, and have B just
        /// as a field in A (and not as an Entry), then we need to explcitly set the list just to A. By default, if
        /// we won't set it, it will result in two entries existing in the Space, A and B, with A having a field of B
        /// as well.
        /// </summary>
        public Type[] InitialLoadEntries
        {
            get { return _initialLoadEntries; }
            set { _initialLoadEntries = value; }
        }

        /// <summary>
        /// Creates Data Enumerators divides to chunk sizes of the given entity type
        /// </summary>
        /// <param name="entityType">Type to enumerate over</param>
        /// <returns>List of enumerators over the given entity</returns>
        protected virtual IEnumerable<IDataEnumerator> DivideToChunks(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentException("DivideToChunks must receive an Entity Type");

            int count = _dbContext.Set(entityType).AsNoTracking().AsQueryable().Count();
            List<IDataEnumerator> enumerators = new List<IDataEnumerator>();
            for (int from = 0; from < count; from += InitialLoadChunkSize)
                enumerators.Add(GetEnumerator(entityType, from, InitialLoadChunkSize));
            return enumerators;
        }

        private void SetOptionsForDbContext(DbContext context)
        {
            context.Configuration.ProxyCreationEnabled = false;
            context.Configuration.LazyLoadingEnabled = false;
            context.Configuration.AutoDetectChangesEnabled = false;
            context.Configuration.ValidateOnSaveEnabled = false;
        }
    }
}
