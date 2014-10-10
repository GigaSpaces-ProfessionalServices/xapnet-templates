Add-Type -A 'System.IO.Compression.FileSystem'
[IO.Compression.ZipFile]::CreateFromDirectory('MultiprojectNhibernate', 'MultiprojectNhibernate.zip')