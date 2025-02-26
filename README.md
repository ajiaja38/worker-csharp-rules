### HIOTO WORKER SMARTHOME LOCAL SERVER .NET

This is the worker that will be running on the local machine.

### How to build?

1. For Windows run `dotnet publish worker_smarthome_local_server.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true`

2. For Linux run `dotnet publish worker_smarthome_local_server.csproj -c Release -r linux-x64 --self-contained true -p:PublishSingleFile=true`

3. For macOS run `dotnet publish worker_smarthome_local_server.csproj -c Release -r osx-x64 --self-contained true -p:PublishSingleFile=true`
