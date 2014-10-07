using GigaSpaces.Core.Persistency;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Text;

namespace My_App.EntityFramework
{
    internal class EFManager
    {
        private DbContext _dbContext;
        protected Dictionary<string, Type> _tableNameToTypeMapping = new Dictionary<string, Type>();
        protected Dictionary<Type, string> _typeToTableNameMapping = new Dictionary<Type, string>();
        private Dictionary<Type, EntitySetBase> _mappingCache = new Dictionary<Type, EntitySetBase>();

        protected Type[] _dbEntries;

        public Type[] DbEntries
        {
            get { return _dbEntries; }
        }

        public EFManager(DbContext context)
        {
            _dbContext = context;
        }

        public virtual void ReloadDbEntries()
        {
            _tableNameToTypeMapping.Clear();
            _typeToTableNameMapping.Clear();

            var collection = ((IObjectContextAdapter)_dbContext).ObjectContext.MetadataWorkspace.GetItems<EntityType>(DataSpace.CSpace);
            var dbTypes = new Type[collection.Count];

            for (var x = 0; x < collection.Count; x++)
            {
                MetadataProperty property;
                if (collection[x].MetadataProperties.TryGetValue("http://schemas.microsoft.com/ado/2013/11/edm/customannotation:ClrType", true, out property))
                {
                    dbTypes[x] = (Type)property.Value;
                }
            }

            foreach (var type in dbTypes)
            {
                string dbTableName = GetTableNameByType(type);
                _tableNameToTypeMapping.Add(dbTableName, type);
                _typeToTableNameMapping.Add(type, dbTableName);
            }
            _dbEntries = dbTypes;
        }

        public virtual string GetTableNameByType(Type type)
        {
            if (_typeToTableNameMapping.ContainsKey(type))
                return _typeToTableNameMapping[type];

            var objectContext = ((IObjectContextAdapter)_dbContext).ObjectContext;
            EntitySetBase es = GetEntitySetByType(type, objectContext);
            return es.Table;
        }

        public virtual Type GetTypeByTableName(string tableName)
        {
            if (_tableNameToTypeMapping.ContainsKey(tableName))
                return _tableNameToTypeMapping[tableName];
            return null;
        }

        private EntitySetBase GetEntitySetByType(Type type, ObjectContext objectContext)
        {
            if (_mappingCache.ContainsKey(type))
                return _mappingCache[type];

            type = ObjectContext.GetObjectType(type);
            string baseTypeName = type.BaseType.Name;
            string typeName = type.Name;

            var es = objectContext.MetadataWorkspace
                            .GetItemCollection(DataSpace.SSpace)
                            .GetItems<EntityContainer>()
                            .SelectMany(c => c.BaseEntitySets
                                            .Where(e => e.Name == typeName
                                            || e.Name == baseTypeName))
                            .FirstOrDefault();

            if (es == null)
                throw new ArgumentException("Entity type not found in GetEntitySet", typeName);

            if (!_mappingCache.ContainsKey(type))
                _mappingCache.Add(type, es);

            return es;
        }

        public virtual Type GetEntityTypeFromSqlQuery(string sqlQuery)
        {
            var tableName = ParseTableNameFromSql(sqlQuery);
            foreach (var name in tableName)
            {
                var type = GetTypeByTableName(name);
                if (type != null)
                    return type;
            }
            return null;
        }

        private List<string> ParseTableNameFromSql(string sql)
        {
            int tableNameStartIndex = sql.LastIndexOf("From ", StringComparison.OrdinalIgnoreCase) + 5;
            int tableNameEndIndex = sql.IndexOf(' ', tableNameStartIndex);
            string tableName = sql.Substring(tableNameStartIndex, tableNameEndIndex - tableNameStartIndex);
            if (tableName.Contains('.'))
            {
                int lastIndexOfDotSymbol = tableName.LastIndexOf('.');
                tableName = tableName.Substring(lastIndexOfDotSymbol + 1);
            }
            return GetPossibleTableRealNames(tableName);
        }

        private List<string> GetPossibleTableRealNames(string tableName)
        {
            var names = new List<string>();
            names.Add(tableName);
            if (tableName.EndsWith("s"))
                names.Add(tableName.Remove(tableName.Length - 1, 1));
            else
                names.Add(tableName + "s");
            return names;
        }

        public Query UpdateQueryFormat(Query query)
        {
            var realType = GetEntityTypeFromSqlQuery(query.SqlQuery);
            var realTableName = _typeToTableNameMapping[realType];
            string sql = UpdateSqlQueryParameterStubs(query.SqlQuery);

            int tableNameStartIndex = sql.LastIndexOf("From ", StringComparison.OrdinalIgnoreCase) + 5;
            int tableNameEndIndex = sql.IndexOf(' ', tableNameStartIndex);
            string tableNameInQuery = sql.Substring(tableNameStartIndex, tableNameEndIndex - tableNameStartIndex);

            sql = sql.Remove(tableNameStartIndex, tableNameEndIndex - tableNameStartIndex);
            sql = sql.Insert(tableNameStartIndex, realTableName);

            if (!sql.StartsWith("select *", StringComparison.OrdinalIgnoreCase))
            {
                sql = "select * " + sql.TrimStart(' ');
            }
            return new Query(query.Parameters, sql);
        }

        private string UpdateSqlQueryParameterStubs(string query)
        {
            var sb = new StringBuilder(query);
            int paramIndex = 0;
            int stringIndex = 0;
            while (true)
            {
                if (stringIndex >= sb.Length)
                    break;
                if (sb[stringIndex] == '?')
                {
                    sb.Replace("?", "{" + paramIndex + "}", stringIndex, 1);
                    paramIndex++;
                }
                stringIndex++;
            }
            return sb.ToString();
        }
    }
}
