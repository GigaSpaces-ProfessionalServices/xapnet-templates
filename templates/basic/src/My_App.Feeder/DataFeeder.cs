using System;
using GigaSpaces.Core;
using log4net;
using My_App.Entities;

namespace My_App.Feeder
{
    public class DataFeeder
    {
        private static readonly ILog Logger = LogManager.GetLogger("feeder");

        public static void Main(string[] applicationArguments)
        {
            Arguments arguments;
            if (Initialize(applicationArguments, out arguments)) return;

            var spaceProxy = GigaSpacesFactory.FindSpace(arguments.SpaceUrl);

            CreateAndWriteRecords(arguments, spaceProxy);
            Logger.InfoFormat("Finished writing {0} record(s) successfully.", arguments.ItemsToAdd);
        }

        private static void CreateAndWriteRecords(Arguments arguments, ISpaceProxy spaceProxy)
        {
            var records = new Datum[arguments.ItemsToAdd];
            Logger.InfoFormat("Writing {0} record(s) to the space.", arguments.ItemsToAdd);

            for (uint x = 0; x < (arguments.ItemsToAdd - 1); x++)
            {
                var datum = new Datum
                {
                    LastUpdatedUtc = DateTime.UtcNow,
                    RouteId = Convert.ToInt32(x)
                };

                records[x] = datum;
            }

            spaceProxy.WriteMultiple(records, WriteModifiers.OneWay);
        }

        private static bool Initialize(string[] applicationArguments, out Arguments arguments)
        {
            var isValidInitialization = false;

            arguments = new Arguments();
            arguments.Parse(applicationArguments);

            Logger.Info("Starting Data Feeder...");

            if (arguments.AreInvalid)
            {
                Logger.Error("Invalid command structure.");
                Logger.Error("My_App.Feeder.exe <space-url> <items-to-add>");
                isValidInitialization = true;
            }
            else
            {
                Logger.InfoFormat("Space Url: {0}", arguments.SpaceUrl);
                Logger.InfoFormat("Items to add: {0}", arguments.ItemsToAdd);             
            }

            return isValidInitialization;
        }

        private class Arguments
        {
            public string SpaceUrl { get; private set; }

            public uint ItemsToAdd { get; private set; }

            public void Parse(string[] applicationArguments)
            {
                try
                {
                    SpaceUrl = applicationArguments[0];
                    ItemsToAdd = uint.Parse(applicationArguments[1]);
                    AreInvalid = true;
                }
                catch
                {
                    AreInvalid = false;
                }
            }

            public bool AreInvalid { get; private set; }
        }
    }

}