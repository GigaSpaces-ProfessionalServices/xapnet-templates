// Guids.cs
// MUST match guids.h
using System;

namespace Gigaspaces.GigaspacesEfPersistencyInstaller
{
    static class GuidList
    {
        public const string guidGigaspacesEfPersistencyInstallerPkgString = "f123ffb0-a8bb-4e89-8b23-29749fefd1be";
        public const string guidGigaspacesEfPersistencyInstallerCmdSetString = "436793e1-825c-4c94-b7b9-cfafd008db13";

        public static readonly Guid guidGigaspacesEfPersistencyInstallerCmdSet = new Guid(guidGigaspacesEfPersistencyInstallerCmdSetString);
    };
}