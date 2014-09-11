using System;
using System.IO;
using GigaSpaces.Core;
using GigaSpaces.XAP.ProcessingUnit.Containers;

namespace My_App.PuDebugExecuter
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(
    "Press enter to start the processing units and press enter again to stop the processing units");
            Console.ReadLine();

            ClusterInfo primaryClusterInfo = new ClusterInfo("partitioned-sync2backup", 1, null, 1, 1);
            ClusterInfo backupClusterInfo = new ClusterInfo("partitioned-sync2backup", 1, 1, 1, 1);

            String deployPath = Path.GetFullPath(@"..\..\..\Deploy");
            ProcessingUnitContainerHost primaryProcessorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "stockprocessor"), primaryClusterInfo, null);
            ProcessingUnitContainerHost backupProcessorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "stockprocessor"), backupClusterInfo, null);
            ProcessingUnitContainerHost mirrorContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "stockmirror"), null, null);
            ProcessingUnitContainerHost feederContainerHost = new ProcessingUnitContainerHost(Path.Combine(deployPath, "stockfeeder"), null, null);

            Console.ReadLine();
            feederContainerHost.Dispose();
            backupProcessorContainerHost.Dispose();
            primaryProcessorContainerHost.Dispose();
            mirrorContainerHost.Dispose();
        }
    }
}
