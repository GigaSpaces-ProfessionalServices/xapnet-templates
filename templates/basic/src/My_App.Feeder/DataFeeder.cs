using System;
using GigaSpaces.Core;
using log4net;
using log4net.Config;
using My_App.Entities;

namespace My_App.Feeder
{
    public class DataFeeder
    {
        private static readonly ILog Logger = LogManager.GetLogger("feeder");

        public static void Main(string[] applicationArguments)
        {
            XmlConfigurator.Configure();

            Arguments arguments;
            if (!Initialize(applicationArguments, out arguments))
            {
                Logger.Info("Press a key to exit.");
                Console.ReadKey();
                return;
            }

            var spaceProxy = GigaSpacesFactory.FindSpace(arguments.SpaceUrl);

            CreateAndWriteRecords(arguments, spaceProxy);
            Logger.InfoFormat("Finished writing {0} record(s) successfully.", arguments.ItemsToAdd);
        }

        private static void CreateAndWriteRecords(Arguments arguments, ISpaceProxy spaceProxy)
        {
            var records = new Data[arguments.ItemsToAdd];
            Logger.InfoFormat("Writing {0} record(s) to the space.", arguments.ItemsToAdd);

            for (uint x = 0; x < arguments.ItemsToAdd; x++)
            {
                var datum = new Data
                {
                    IsProcessed = false,
                    RawContent = string.Format("FEEDER: {0}", DateTime.UtcNow.Ticks),
                    Type = Convert.ToInt64(x)
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

            if (arguments.Invalid)
            {
                Logger.Error("Invalid command structure.");
                Logger.Error("My_App.Feeder.exe <space-url> <items-to-add>");
            }
            else
            {
                Logger.InfoFormat("Space Url: {0}", arguments.SpaceUrl);
                Logger.InfoFormat("Items to add: {0}", arguments.ItemsToAdd);
                isValidInitialization = true;
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
                    Invalid = false;
                }
                catch
                {
                    Invalid = true;
                }
            }

            public bool Invalid { get; private set; }
        }
    }

}