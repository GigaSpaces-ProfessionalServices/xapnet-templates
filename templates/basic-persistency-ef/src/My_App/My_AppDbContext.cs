using My_App.Entities;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;

namespace My_App
{
    public class My_AppDbContext:DbContext
    {
        public My_AppDbContext()
            : base("name=My_AppConnectionString")
        { }

        public My_AppDbContext(string connectionString)
            : base(connectionString)
        { }

        public virtual DbSet<Data> Data { get; set; }
    }
}
