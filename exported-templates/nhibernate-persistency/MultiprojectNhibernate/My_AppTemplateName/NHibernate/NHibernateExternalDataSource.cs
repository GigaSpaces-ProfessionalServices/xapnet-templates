using System;
using System.Collections.Generic;
using GigaSpaces.Core.Persistency;
using $saferootprojectname$.NHibernate.Enumerator;
using NHibernate;
using NHibernate.Criterion;
using NHibernate.Metadata;
using NHibernate.Persister.Entity;

namespace $saferootprojectname$.NHibernate
{
	/// <summary>
	/// GigaSpaces implementation of IExternalDataSource using NHibernate 
	/// </summary>
	public class NHibernateExternalDataSource : AbstractExternalDataSource
	{
		#region Members
		// NHibernate specific properties
		public const string NHibernateConfigProperty = "nhibernate-config-file";
		public const string NHibernateHbmDirectory = "nhibernate-hbm-dir";
		public const string NHibernateConnectionStringProperty = "nhibernate-connection-string";

		private ISessionFactory _sessionFactory;
		private int _enumeratorLoadFetchSize = 10000;
		private string[] _initialLoadEntries;
		private string[] _managedEntries;
		private Dictionary<string, string> _managedEntriesDictionary;
		private int _initialLoadChunkSize = 100000;
		private bool _performOrderById = false;
		private int _initialLoadThreadPoolSize = 10;
		private bool _useMerge = false;
		#endregion

		#region Constructors

		/// <summary>
		/// Create a new NHibernate External Data Source
		/// </summary>
		public NHibernateExternalDataSource()
		{
		}
		/// <summary>
		/// Create a new NHibernate External Data Source
		/// </summary>
		/// <param name="sessionFactory">NHibernate Session Factory that will be used</param>
		public NHibernateExternalDataSource(ISessionFactory sessionFactory)
		{
			_sessionFactory = sessionFactory;
		}

		#endregion

		#region ISqlDataSource Members

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
			_useMerge = GetBoolProperty("UseMerge", _useMerge);

			// only configure a session factory if it is not injected			
			if (_sessionFactory == null)
			{
				string nhibernateFile = GetFileProperty(NHibernateConfigProperty);
				string hbmDirectory = GetFileProperty(NHibernateHbmDirectory);
				string connectionString = GetProperty(NHibernateConnectionStringProperty);
				_sessionFactory = SessionFactoryBuilder.GetFactory(nhibernateFile, hbmDirectory, connectionString);
			}
			// only extract managed entries if it wasn't injected
			if (_managedEntries == null)
			{
				List<string> managedEntriesList = new List<string>();
				IDictionary<string, IClassMetadata> allClassMetadata = _sessionFactory.GetAllClassMetadata();
				foreach (string type in allClassMetadata.Keys)
				{
					managedEntriesList.Add(type);
				}
				ManagedEntries = managedEntriesList.ToArray();
			}
			// only extract initial load entries if it wasn't injected
			if (_initialLoadEntries == null)
			{
				List<string> initialLoadEntriesList = new List<string>();
				IDictionary<string, IClassMetadata> allClassMetadata = _sessionFactory.GetAllClassMetadata();
				foreach (KeyValuePair<string, IClassMetadata> entry in allClassMetadata)
				{
					AbstractEntityPersister entityPersister = (AbstractEntityPersister)entry.Value;
					string mappedSuperClass = entityPersister.MappedSuperclass;
					if (mappedSuperClass != null)
					{
						IClassMetadata superClassMetadata = allClassMetadata[mappedSuperClass];
						if (superClassMetadata.GetMappedClass(EntityMode.Map) != null)
						{
							//Filter out those who have their super classes mapped
							continue;
						}
					}
					initialLoadEntriesList.Add(entry.Key);
				}
				InitialLoadEntries = initialLoadEntriesList.ToArray();
			}
		}

		/// <summary>
		/// Close the data source and clean any used resources.
		/// Called before space shutdown.
		/// </summary>
		public override void Shutdown()
		{
			try
			{
				if (_sessionFactory != null)
					_sessionFactory.Close();
			}
			catch
			{
			}

			base.Shutdown();
		}

		///<summary>
		///Create an enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.		            
		///</summary>
		///<param name="query">The Query used for matching.</param>
		///<returns>
		///Enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.
		///</returns>
		///
		public override IDataEnumerator GetEnumerator(Query query)
		{
			return new NHibernateDataEnumerator(query, SessionFactory, EnumeratorLoadFetchSize, PerformOrderById);
		}

		/// <summary>
		/// Creates and returns an enumerator over all the entries that should be loaded into space.
		/// </summary>
		/// <returns>Enumerator over all the entries that should be loaded into space.</returns>
		public override IDataEnumerator InitialLoad()
		{
			List<IDataEnumerator> enumerators = new List<IDataEnumerator>();
			foreach (string initialLoadEntity in InitialLoadEntries)
			{
				if (InitialLoadChunkSize == -1)
					enumerators.Add(GetEnumerator(initialLoadEntity, 0, int.MaxValue));
				else
					enumerators.AddRange(DivideToChunks(initialLoadEntity));
			}

			return new ConcurrentMultiDataEnumerator(enumerators, EnumeratorLoadFetchSize, InitialLoadThreadPoolSize);
		}

		/// <summary>
		/// Execute given bulk of operations.
		/// Each BulkItem contains one of the following operation -
		///
		/// WRITE - given object should be inserted to the data store,
		/// UPDATE - given object should be updated in the data store,
		/// REMOVE - given object should be deleted from the data store
		///
		/// If the implementation uses transactions,
		/// all the bulk operations must be executed in one transaction.
		/// </summary>
		/// <param name="bulk">Collection of bulk items to execute</param>
		public override void ExecuteBulk(IList<BulkItem> bulk)
		{
			ExecuteBulk(bulk, 0);
		}

		#endregion

		#region Properties

		/// <summary>
		/// Gets the session factory used to interoperate with nHibernate.
		/// </summary>
		public ISessionFactory SessionFactory
		{
			get { return _sessionFactory; }
		}

		/// <summary>
		/// Gets or sets the InitialLoad operation thread pool size. The InitialLoad operation uses the <see cref="ConcurrentMultiDataEnumerator"/>.
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
		public string[] InitialLoadEntries
		{
			get { return _initialLoadEntries; }
			set { _initialLoadEntries = value; }
		}

		/// <summary>
		/// Gets or sets the entry types this NHibernate data source will work with. By default, will use NHiberante meta
		/// data API in order to get the list of all the given entities it handles.
		///
		/// This list is used to filter out entities when performing all data source operations exception for
		/// the InitialLoad operation.
		/// </summary>
		public string[] ManagedEntries
		{
			get { return _managedEntries; }
			set
			{
				_managedEntries = value;
				_managedEntriesDictionary = new Dictionary<string, string>();
				if (value != null)
				{
					foreach (string managedEntry in ManagedEntries)
					{
						//Initialized Managed Entries Dictionary to be used later for faster search
						_managedEntriesDictionary.Add(managedEntry, managedEntry);
					}
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
		/// Gets or sets the enumerator fetch size
		/// </summary>        
		public int EnumeratorLoadFetchSize
		{
			get { return _enumeratorLoadFetchSize; }
			set
			{
				if (value < 1)
					throw new ArgumentException("EnumeratorLoadFetchSize must be a positive number");
				_enumeratorLoadFetchSize = value;
			}
		}

		/// <summary>
		/// Gets or sets the use merge flag which indicates whether to use NHibernate's
		/// merge to perform the create/update
		/// </summary>
		public bool UseMerge
		{
			get { return _useMerge; }
			set { _useMerge = value; }
		}

		#endregion

		#region Methods

		/// <summary>
		/// Create an enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.		            
		/// </summary>
		/// <param name="entityType">The entity type to match.</param>
		/// <param name="from">Base index to load from. If null, loads everything.</param>
		/// <param name="maxResults">Maximum results to return</param>
		/// <returns>
		/// Enumerator over all objects that match the given <see cref="T:GigaSpaces.Core.Persistency.Query" />.
		/// </returns>
		protected virtual IDataEnumerator GetEnumerator(String entityType, int from, int maxResults)
		{
			return new NHibernateDataEnumerator(entityType, SessionFactory, EnumeratorLoadFetchSize, PerformOrderById, from, maxResults);
		}

		/// <summary>
		/// Execute given bulk of operations.
		/// </summary>
		/// <param name="bulk">Collection of bulk items to execute</param>
		/// <param name="retries">Number of retries already attempted</param>
		protected virtual void ExecuteBulk(IList<BulkItem> bulk, int retries)
		{
			ISession session = _sessionFactory.OpenSession();
			ITransaction tx = null;

			try
			{
				tx = session.BeginTransaction();
				foreach (BulkItem bulkItem in bulk)
					ExecuteBulkItem(bulkItem, session, retries);
				tx.Commit();
				CloseSession(session);
			}
			catch (Exception e)
			{
				if (tx != null)
					tx.Rollback();
				CloseSession(session);
				if (retries > 0)
					throw new Exception("Can't execute bulk store.", e);
				ExecuteBulk(bulk, retries + 1);
			}
		}

		/// <summary>
		/// Executes a singly bulk item operation
		/// </summary>
		/// <param name="bulkItem">Bulk item to execute</param>
		/// <param name="session">Session to use to execute the bulk item</param>
		/// <param name="retries">Number of retries already attempted</param>
		protected virtual void ExecuteBulkItem(BulkItem bulkItem, ISession session, int retries)
		{
			object entry = bulkItem.Item;

			if (!_managedEntriesDictionary.ContainsKey(entry.GetType().ToString()))
				return;

			switch (bulkItem.Operation)
			{
				case BulkOperation.Remove:
					session.Delete(session.Merge(entry));
					break;
				case BulkOperation.Write:
				case BulkOperation.Update:
					if (retries > 0 || _useMerge)
						session.Merge(entry);
					else
					{
						try
						{
							session.SaveOrUpdate(entry);
						}
						catch (HibernateException)
						{
							session.Merge(entry);
						}
					}
					break;
				default:
					break;
			}
		}

		/// <summary>
		/// Creates Data Enumerators divides to chunk sizes of the given entity type
		/// </summary>
		/// <param name="entityType">Type to enumerate over</param>
		/// <returns>List of enumerators over the given entity</returns>
		protected virtual IEnumerable<IDataEnumerator> DivideToChunks(string entityType)
		{
			if (entityType == null)
				throw new ArgumentException("DivideToChunks must receive an Entity Type");

			ISession session = _sessionFactory.OpenSession();
			ITransaction tx = session.BeginTransaction();
			try
			{
				//Get number of rows of the current entity in the data base
				ICriteria criteria = session.CreateCriteria(entityType);
				criteria.SetProjection(Projections.RowCount());
				int count = (int)criteria.UniqueResult();
				//Create enumerators for each chunk
				List<IDataEnumerator> enumerators = new List<IDataEnumerator>();
				for (int from = 0; from < count; from += InitialLoadChunkSize)
					enumerators.Add(GetEnumerator(entityType, from, InitialLoadChunkSize));
				return enumerators;
			}
			finally
			{
				if (tx != null && tx.IsActive)
					tx.Commit();
				CloseSession(session);
			}
		}

		/// <summary>
		/// Closes an NHibernate session
		/// </summary>
		/// <param name="session">Session to close</param>
		private static void CloseSession(ISession session)
		{
			if (session.IsOpen)
				session.Close();
		}

		#endregion
	}
}
