using System;
using GigaSpaces.Core;
using GigaSpaces.XAP.Events;
using GigaSpaces.XAP.Events.Polling;
using My_App.Entities;

namespace My_App.Processor
{
    [PollingEventDriven]
    public class DataProcessor
    {
        [EventTemplate]
        public SqlQuery<Datum> UnprocessedData
        {
            get
            {
                var templateQuery = new SqlQuery<Datum>("Content = null");

                return templateQuery;
            }
        }

        [DataEventHandler]
        public Datum CreateData(Datum datum)
        {
            var random = new Random();
            datum.Content = new byte[random.Next()];
            random.NextBytes(datum.Content);
            datum.LastUpdatedUtc = DateTime.UtcNow;

            return datum;
        }
    }
}