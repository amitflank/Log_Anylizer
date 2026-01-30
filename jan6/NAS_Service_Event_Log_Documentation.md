# NAS Service Event Log Documentation

## 1. Purpose and Scope
This log records the runtime events of the NAS service, which provides NFS and SMB access. Unlike the system snapshot logs (which capture state at fixed intervals), the NAS log is **event-driven**.

Its purposes are:
- To trace the lifecycle of NAS subsystems (NFS, SMB, Winbind, RPC, etc.).
- To capture status changes (services starting, stopping, reloading).
- To provide diagnostic information for troubleshooting failures.
- To serve as an event stream that can be analyzed by monitoring or machine learning systems to distinguish between healthy operation and anomalous behavior.

---

## 2. Log Format

Each line has the following structure:

```
2024-10-25 01:44:23.014 +00:00 [Information] [NAS                 ] [T=12,ET=] Opening connection...
```

**Fields**
- **Timestamp** – UTC time when the event occurred.  
- **Log Level** – Severity of the event. Standard .NET levels: `Critical`, `Error`, `Warning`, `Information`, `Debug`, `Verbose` (sometimes `Trace`).  
- **Component** – Source module emitting the message.  
- **Thread Context** – Thread ID and execution token (e.g., `[T=12,ET=]`).  
- **Message** – Free-text description of the event.  

If the component field is blank in a `Debug` entry and the message contains `StopwatchScope`, the component is interpreted as **StopwatchScope**. Otherwise, it is recorded as **Unknown**.

---

## 3. Log Levels
The NAS log uses standard .NET log levels to classify events. In this log, the following levels appear:

- **Error** – Indicates a failed operation.  
- **Warning** – Indicates a potential issue or unexpected condition.  
- **Information** – Normal lifecycle messages (e.g., connections, mounts).  
- **Debug** – Diagnostic events showing internal operations.  
- **Verbose** – Fine-grained trace information for detailed troubleshooting.  
- **Trace** – Occasional very low-level events such as heartbeats.  

**Special note on Debug events:**  
During analysis, many `Debug` log lines did not contain a component name in the expected field. However, their messages consistently included `StopwatchScope: ...`, indicating that they were timing instrumentation events. For consistency, these entries are documented under the **StopwatchScope** component.  

---

## 4. Components Overview
Note: all of these were extracted form NAS log file(s) and therefore may not represent the extent of components actually in the system. We should expect that events that are rare will be excluded from a scrape of a few files. We can be more confident in the levels as they seem to map to C# sharp levels. 

- **NAS** – core NAS service logic.  
- **NasManager** – subsystem executing commands and performing NAS-related operations.  
- **ObjectStore** – backend object storage operations, including configuration and provisioning steps.  
- **SCF** – service communication framework for registering and managing connections.  
- **TRACE** – tracing and heartbeat component.  
- **StopwatchScope** – instrumentation for measuring operation timings.  
- **ConfigurationFileWatcher** – monitors and parses configuration files.  
- **SCSIGateway** – SCSI gateway subsystem; events include NIC/environment checks and target handling.  
- **NfsVaaiRest** – handles VMware VAAI extensions for NFS; manages REST endpoints.  

---

## 5. Level × Component Examples

Below are representative examples for each observed combination of level and component.

### Error – NasManager
```
2024-10-25 01:44:23.211 +00:00 [Error      ] [NasManager] [T=8,ET=] ProcessExecutor.Execute: Execution failed after 1 retries
```
- **Level**: Error  
- **Component**: NasManager  
- **Message**: command execution failed after retries.  

### Error – ObjectStore
```
2024-10-25 02:23:05.083 +00:00 [Error      ] [ObjectStore         ] [T=28,ET=] CreateConfigFile: entityRootConfigPath path does not exists : [/volumes/Vol-1138/exports/.swift.sys] creating now.
```
- **Level**: Error  
- **Component**: ObjectStore  
- **Message**: entityRootConfigPath path does not exists 

### Warning – NasManager
```
2024-10-25 01:48:24.548 +00:00 [Warning    ] [NasManager] [T=17,ET=] SetValue: Path '/sys/block/drbd2005/device/queue_depth' does not exist. (9 retries left...)
```
- **Level**: Warning  
- **Component**: NasManager  
- **Message**: missing kernel/sysfs path during operation.  

### Information – ConfigurationFileWatcher
```
2024-10-25 01:44:23.106 +00:00 [Information] [ConfigurationFileWatcher] [T=8,ET=] GetConfigurationChanges started, comparing current to the value saved on the first boot.
```
- **Level**: Information  
- **Component**: ConfigurationFileWatcher  
- **Message**: starting configuration change comparison.  

### Information – NAS
```
2024-10-25 01:44:23.053 +00:00 [Information] [NAS] [T=8,ET=] OpenConnection done.
```
- **Level**: Information  
- **Component**: NAS  
- **Message**: connection completed.  

### Information – ObjectStore
```
2024-10-25 01:48:25.197 +00:00 [Information] [ObjectStore         ] [T=17,ET=] ChangeObjectStoreSecrets done for /volumes/Vol-1257/exports/.swift.sys/proxy-server.conf
 ```
- **Level**: Information  
- **Component**: ObjectStore  
- **Message**:  ChangeObjectStoreSecrets done for ...

### Information – SCF
```
2024-10-25 01:44:23.052 +00:00 [Information] [SCF] [T=12,ET=] SCFConnection RegisterName: Connection 52253787: "ScfConnectionHelper<IStorOneCloud>" registered (count=1).
```
- **Level**: Information  
- **Component**: SCF  
- **Message**: registering SCF connection.  

### Debug – ConfigurationFileWatcher
```
2024-10-25 01:44:23.082 +00:00 [Debug      ] [ConfigurationFileWatcher] [T=8,ET=] /home/.../StorONE.dll.config is a link. Using the real file's path.
```
- **Level**: Debug  
- **Component**: ConfigurationFileWatcher  
- **Message**: resolving symlink for configuration file.  

### Debug – NAS
```
2024-10-25 01:44:23.076 +00:00 [Debug      ] [NAS] [T=8,ET=] ThreadPoolKeepAlive created
```
- **Level**: Debug  
- **Component**: NAS  
- **Message**: creation of a keepalive process.  

### Debug – NasManager
```
2024-10-25 01:44:23.178 +00:00 [Debug      ] [NasManager          ] [T=8,ET=] NASConfig: Not supporting multipath. concurrent SessionsCount=1
```
- **Level**: Debug  
- **Component**: NasManager  
- **Message**: Not supporting multipath. concurrent SessionsCount=1

### Debug – ObjectStore
```
2024-10-25 02:23:04.807 +00:00 [Debug      ] [ObjectStore         ] [T=28,ET=] NASManager. AddObjectStore. creating fullSharePath("/volumes/Vol-1138/exports")
```
- **Level**: Debug  
- **Component**: ObjectStore  
- **Message**: NASManager. AddObjectStore. creating fullSharePath ...

### Debug – StopwatchScope
```
[Debug      ] [] [T=126,ET=] StopwatchScope: "HandleCommitConfigurationChanges(5519320880359759031)" completed 0.006ms
```
- **Level**: Debug  
- **Component**: StopwatchScope  
- **Message**: "HandleCommitConfigurationChanges(5519320880359759031)" completed 0.006ms

### Verbose – ConfigurationFileWatcher
```
2024-10-25 01:44:23.090 +00:00 [Verbose    ] [ConfigurationFileWatcher] [T=8,ET=] Parsed CSV file has 236 properties.
```
- **Level**: Verbose  
- **Component**: ConfigurationFileWatcher  
- **Message**: reporting parsed properties from configuration.  

### Verbose – NAS
```
2024-10-25 01:44:23.075 +00:00 [Verbose    ] [NAS] [T=8,ET=] WatchdogInstance.ctor Settings="ExpirationMilliseconds: 300000, TimerName=NAS-ThreadPoolKeepAliveSensor"
```
- **Level**: Verbose  
- **Component**: NAS  
- **Message**: constructing watchdog instance.  

### Verbose – NasManager
```
2024-10-25 01:44:23.191 +00:00 [Verbose    ] [NasManager          ] [T=8,ET=] StopwatchScope: "ProcessExecutor.Execute: [/bin/bash -c \"/usr/bin/smbcontrol smbd ping\"]" started...
```
- **Level**: Verbose  
- **Component**: NasManager  
- **Message**: starting execution of an smbd ping comand. 

### Verbose – NfsVaaiRest
```
2024-10-25 01:48:23.024 +00:00 [Verbose    ] [NfsVaaiRest] [T=8,ET=] Starting NFS VAAI REST endpoint on ...
```
- **Level**: Verbose  
- **Component**: NfsVaaiRest  
- **Message**: starting NFS VAAI REST endpoint.  

### Verbose – ObjectStore
```
2024-10-25 02:23:04.807 +00:00 [Verbose    ] [ObjectStore         ] [T=28,ET=] Using Swift object store.
```
- **Level**: Verbose  
- **Component**: ObjectStore  
- **Message**: Using Swift object store.  

### Verbose – SCSIGateway
```
2024-10-25 01:48:22.967 +00:00 [Verbose    ] [SCSIGateway] [T=8,ET=] ScstConfig.FindTenGigInterfaces: interface="MGMT"
```
- **Level**: Verbose  
- **Component**: SCSIGateway  
- **Message**: inspecting interface configuration.  

### Verbose – TRACE
```
2024-10-25 01:44:23.044 +00:00 [Verbose    ] [TRACE] [T=12,ET=] SCFClient.HeartbeatTimer Initialized HeartbeatInterval=30000
```
- **Level**: Verbose  
- **Component**: TRACE  
- **Message**: initializing heartbeat timer.  

---

## 6. Event Categories

Events can also be grouped into functional categories:

### 6.1 Connection Setup  
- **Components**: `NAS`, `SCF`, `TRACE`  
- **Examples**: opening connections, registering SCF clients, initializing heartbeat timers.  

### 6.2 Service Watchdogs & Keepalives  
- **Components**: `NAS`, `StopwatchScope`  
- **Examples**: watchdog creation (`ThreadPoolKeepAliveSensor`), timers monitoring service health, StopwatchScope timings.  

### 6.3 Configuration Management  
- **Component**: `ConfigurationFileWatcher`  
- **Examples**: resolving symlinks, parsing configuration files, tracking property changes.  

### 6.4 Process Execution & Management  
- **Component**: `NasManager`  
- **Examples**: executing external commands (`smbcontrol`, `systemctl`, `ethtool`), enabling maintenance features (`fstrim`).  

### 6.5 Storage Backend Operations  
- **Component**: `ObjectStore`  
- **Examples**: updating secrets, provisioning Swift helper scripts, creating configuration files, backend write attempts.  

### 6.6 Protocol Extensions  
- **Components**: `SCSIGateway`, `NfsVaaiRest`  
- **Examples**: inspecting NICs, starting VMware VAAI REST endpoints.  
