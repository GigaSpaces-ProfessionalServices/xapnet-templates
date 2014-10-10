using System;
using System.Collections.Generic;

namespace $saferootprojectname$.Feeder
{
    public class FeederConfiguration
    {
        public FeederConfiguration(IDictionary<string, string> properties)
        {
            NumberOfRecords = Convert.ToInt32(properties["NumberOfRecords"]);
            FeedingThrottle = Convert.ToInt32(properties["FeedingThrottle"]);
            BlockSize = Convert.ToInt32(properties["BlockSize"]);
        }

        public int NumberOfRecords { get; set; }
        public int FeedingThrottle { get; set; }
        public int BlockSize { get; set; }
    }
}