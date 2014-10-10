using System;
using System.IO;
using GigaSpaces.Core;
using GigaSpaces.XAP.ProcessingUnit.Containers;

namespace $saferootprojectname$.PuDebugExecuter
{
    class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine(
                "Press enter to start the processing units and press enter again to stop the processing units");
            Console.ReadLine();

            var primaryClusterInfo = new ClusterInfo("partitioned-sync2backup", 1, null, 1, 1);
            var backupClusterInfo = new ClusterInfo("partitioned-sync2backup", 1, 1, 1, 1);

            var deployPath = Path.GetFullPath(@"..\");
            var primaryProcessorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "processor"), primaryClusterInfo, null);
            var backupProcessorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "processor"), backupClusterInfo, null);
            var mirrorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "mirror"), null, null);
            var feederContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "feeder"), null, null);

            Console.ReadLine();
            feederContainerHost.Dispose();
            backupProcessorContainerHost.Dispose();
            primaryProcessorContainerHost.Dispose();
            mirrorContainerHost.Dispose();
        }
    }
}
