using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using GigaSpaces.Core;
using GigaSpaces.XAP.ProcessingUnit.Containers.BasicContainer;
using My_App.Entities;

namespace My_App.Feeder
{
    [BasicProcessingUnitComponent(Name = "FillFeeder")]
    public class FeederProcessor : IDisposable
    {
        #region Members

        private Data[] _fills;
        private ISpaceProxy _proxy;
        private Thread _feederThread;
        private volatile bool _continueFeeding;
        private FeederConfiguration _config;

        #endregion

        [ContainerInitialized]
        public void Initialize(BasicProcessingUnitContainer container)
        {
            _proxy = container.GetSpaceProxy("stock");
            _config = new FeederConfiguration(container.Properties);
            _continueFeeding = true;

            _fills = new Data[_config.NumberOfRecords];

            GenerateFills(_fills);
            _proxy.Snapshot(new Data());

            _feederThread = new Thread(Feed);
            _feederThread.Start();
        }

        /// <summary>
        /// This method breaks the generated fills collections to blocks of a configured size
        ///  and feeds them (WriteMultiple) to the space by blocks.
        /// </summary>
        public void Feed()
        {
            try
            {
                //Stopwatch is used to measure feed time.
                var stopWatch = new Stopwatch();

                var blocks = _fills.Length / _config.BlockSize;
                var buffer = new Data[_config.BlockSize];

                stopWatch.Start();
                for (int block = 0; block < blocks; block++)
                {
                    //Copy the current fiils block
                    int offset = block * _config.BlockSize;
                    Array.Copy(_fills, offset, buffer, 0, buffer.Length);

                    //Write the current block to the space
                    _proxy.WriteMultiple(buffer);

                    Thread.Sleep(_config.FeedingThrottle);

                    if (!_continueFeeding)
                        break;
                }
                stopWatch.Stop();
            }
            catch
            {

            }
        }

        /// <summary>
        /// Stops the feeding process on dispose (Automatically invoked when the hosting container is disposing)
        /// </summary>
        public void Dispose()
        {
            Stop();
            if (_feederThread != null)
            {
                _feederThread.Join(_config.FeedingThrottle + 5000);
                _feederThread = null;
            }
        }

        private void Stop()
        {
            _continueFeeding = false;
        }

        private void GenerateFills(Data[] fills)
        {
            Random rnd = new Random(DateTime.Now.GetHashCode());

            for (int i = 0; i < fills.Length; i++)
            {
                var fill = new Data();
                fill.Type = Convert.ToInt64(rnd.Next());
                fill.RawContent = "FEEDER: " + DateTime.UtcNow.Ticks;
                fill.IsProcessed = false;
                
                fills[i] = fill;
            }
        }
    }

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
