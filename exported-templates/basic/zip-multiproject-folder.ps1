Add-Type -A 'System.IO.Compression.FileSystem'
[IO.Compression.ZipFile]::CreateFromDirectory('MultiprojectBasic', 'MultiprojectBasic.zip')