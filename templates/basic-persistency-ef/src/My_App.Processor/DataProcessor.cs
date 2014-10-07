using GigaSpaces.XAP.Events;
using GigaSpaces.XAP.Events.Polling;
using My_App.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace My_App.Processor
{
    [PollingEventDriven]
    public class DataProcessor
    {
        private const int WorkDuration = 100;

        [EventTemplate]
        public Data UnprocessedData
        {
            get { return new Data { IsProcessed = false }; }
        }

        [DataEventHandler]
        public Data CreateData(Data data)
        {
            Thread.Sleep(WorkDuration);
            data.Content = string.Format("PROCESSED: {0}", data.RawContent);
            data.IsProcessed = true;

            return data;
        }
    }
}
