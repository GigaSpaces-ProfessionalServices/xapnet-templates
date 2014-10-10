using System;
using GigaSpaces.Core.Metadata;

namespace $saferootprojectname$.Entities
{
    [SpaceClass]
    public class Data
    {
        [SpaceID(AutoGenerate = true)]
        public string Id { get; set; }

        [SpaceRouting]
        public long? Type { get; set; }

        public string RawContent { get; set; }

        public string Content { get; set; }

        public bool? IsProcessed { get; set; }
    }
}
