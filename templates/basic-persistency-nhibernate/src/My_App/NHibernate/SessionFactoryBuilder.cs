using System.IO;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Tool.hbm2ddl;

namespace My_App.NHibernate
{
	internal static class SessionFactoryBuilder
	{

        /// <summary>
        /// Returns a session factory
        /// </summary>
        /// <param name="hibernateFile">Configuration hibernate file</param>
        /// <param name="hbmDirectory">Directory containing the hbm files</param>
        /// <param name="connectionString">Provide override connection string</param>
        /// <returns>session factory</returns>
        public static ISessionFactory GetFactory(string hibernateFile, string hbmDirectory, string connectionString)
        {
            try
            {
                string absoluteHibernateFile = System.Environment.ExpandEnvironmentVariables(hibernateFile);
                absoluteHibernateFile = Path.GetFullPath(absoluteHibernateFile);                
                Configuration config = Configure(new Configuration(), absoluteHibernateFile); 
                if (!System.String.IsNullOrEmpty(connectionString))
                    config.SetProperty(Environment.ConnectionString, connectionString);                	            
                if (!System.String.IsNullOrEmpty(hbmDirectory))
                {
                    hbmDirectory = System.Environment.ExpandEnvironmentVariables(hbmDirectory);
                    string absoluteHbmDir = Path.GetFullPath(hbmDirectory);
                    config.AddDirectory(new DirectoryInfo(absoluteHbmDir));
                }
                return config.BuildSessionFactory();
            }
            catch (System.Exception e)
            {
                throw new System.Exception("Error creating NHibernate Session Factory", e);
            }
        }

        /// <summary>
        /// Configure according to hibernate.cfg.xml
        /// </summary>
        /// <param name="config">Configuration to apply the config file to</param>
        /// <param name="hibernateFile">Hibernate file path</param>
        /// <returns>Updated configuration</returns>
        private static Configuration Configure(Configuration config,
                string hibernateFile)
        {
            // In case that hibernate config file location is null find hibernate.cfg.xml
            // file in classpath
            if (hibernateFile == null)
                return config.Configure();

            return config.Configure(hibernateFile);

        }
	}
}
