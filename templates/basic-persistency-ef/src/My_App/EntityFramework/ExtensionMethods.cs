using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace My_App.EntityFramework
{
    public static class ExtensionMethods
    {
        private static Dictionary<Type, IEnumerable<string>> _primaryKeysCache = new Dictionary<Type, IEnumerable<string>>();

        public static IQueryable OrderBy(this IQueryable source, string orderingProperty)
        {
            var type = source.ElementType;
            var property = type.GetProperty(orderingProperty);
            var parameter = Expression.Parameter(type, "p");
            var propertyAccess = Expression.MakeMemberAccess(parameter, property);
            var orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { type, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
            return source.Provider.CreateQuery(resultExp);
        }

        public static IQueryable OrderByDescending(this IQueryable source, string orderingProperty)
        {
            var type = source.ElementType;
            var property = type.GetProperty(orderingProperty);
            var parameter = Expression.Parameter(type, "p");
            var propertyAccess = Expression.MakeMemberAccess(parameter, property);
            var orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderByDescending", new Type[] { type, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
            return source.Provider.CreateQuery(resultExp);
        }

        public static IEnumerable Cast(this IEnumerable source, Type type)
        {
            var castMethod = typeof(Enumerable).GetMethod("Cast", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public);
            var constructedMethodInfo = castMethod.MakeGenericMethod(new Type[] { type });
            var castList = constructedMethodInfo.Invoke(null, new object[] { source });
            return (IEnumerable)castList;
        }

        public static IQueryable Skip(this IQueryable source, int skipCount)
        {
            return Queryable.Skip((dynamic)source, skipCount);
        }

        public static IQueryable Take(this IQueryable source, int takeCount)
        {
            return Queryable.Take((dynamic)source, takeCount);
        }

        public static int Count(this IQueryable source)
        {
            return Queryable.Count((dynamic)source);
        }

        public static object Merge(this DbContext context, object entity)
        {
            var entityType = entity.GetType();
            var tracked = context.Set(entityType).Find(context.KeyValuesFor(entity));
            if (tracked != null)
            {
                context.Entry(tracked).CurrentValues.SetValues(entity);
                return tracked;
            }
            else
            {
                var persistedInstance = context.Set(entityType).Add(entity);
                context.Entry(persistedInstance).State = EntityState.Detached;
                return persistedInstance;
            }
        }

        public static object[] KeyValuesFor(this DbContext context, object entity)
        {
            var entry = context.Entry(entity);
            return context.GetEntityPrimaryKeys(entity.GetType())
                .Select(k => entry.Property(k).CurrentValue)
                .ToArray();
        }

        public static IEnumerable<string> GetEntityPrimaryKeys(this DbContext context, Type entityType)
        {
            if (_primaryKeysCache.ContainsKey(entityType))
                return _primaryKeysCache[entityType];

            var ospaceType = GetEntityType(context, entityType);
            var primaryKeys = ospaceType.KeyMembers.Select(k => k.Name);
            _primaryKeysCache.Add(entityType, primaryKeys);
            return primaryKeys;
        }

        public static IEnumerable<string> GetEntityMembers(this DbContext context, Type entityType)
        {
            var ospaceType = GetEntityType(context, entityType);
            return ospaceType.Members.Where(p => p.BuiltInTypeKind != BuiltInTypeKind.NavigationProperty).Select(k => k.Name);
        }

        private static EntityType GetEntityType(DbContext context, Type entityType)
        {
            Contract.Requires(context != null);
            Contract.Requires(entityType != null);

            entityType = ObjectContext.GetObjectType(entityType);

            var metadataWorkspace =
                ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;
            var objectItemCollection =
                (ObjectItemCollection)metadataWorkspace.GetItemCollection(DataSpace.OSpace);

            var ospaceType = metadataWorkspace
                .GetItems<EntityType>(DataSpace.OSpace)
                .SingleOrDefault(t => objectItemCollection.GetClrType(t) == entityType);

            if (ospaceType == null)
            {
                throw new ArgumentException(
                    string.Format(
                        "The type '{0}' is not mapped as an entity type.",
                        entityType.Name),
                    "entityType");
            }
            return ospaceType;
        }
    }
}
