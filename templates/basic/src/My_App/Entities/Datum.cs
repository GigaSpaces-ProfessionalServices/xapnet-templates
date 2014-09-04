using System;
using GigaSpaces.Core.Metadata;

namespace My_App.Entities
{
    [SpaceClass]
    public class Datum
    {
        [SpaceID(AutoGenerate = true)]
        public string Id { get; set; }

        [SpaceRouting]
        public int RouteId { get; set; }

        public byte[] Content { get; set; }

        public DateTime LastUpdatedUtc { get; set; }
    }
}
