# System Information Log Documentation

## Purpose
This log captures **periodic system state snapshots** of a storage system. It is generated automatically at fixed intervals (e.g., every 5 minutes) and is used for:

- Monitoring system health in real time.  
- Diagnosing storage or replication issues.  
- Feeding structured data into ML pipelines for **performance analysis** and **anomaly detection**.  

Each entry is time-stamped and contains a consistent set of categories.

---

## Log Structure
Every periodic entry follows this structure:

1. **Timestamp** – Date and time of the snapshot.  
2. **Drives, Capacity & Pools** – Status of storage devices and allocation pools.  
   - **Drive Capacity Stats**  
   - **Manual Pools**  
   - **ThinProvisioningStates**  
3. **ConsistencyGroups** – Logical groupings of volumes that must be synchronized.  
4. **Volumes** – Logical storage volumes and their attributes.  
5. **Statistics** – Hardware utilization.  
6. **Rebuild Stats** – Rebuild or recovery activity.  
7. **HA (High Availability)** – Node status, leadership, and peer state.  
8. **Network** – IP addresses assigned to the system.  
9. **Replication Status** – Replication connections and health between nodes.  

---

## Drives, Capacity & Pools

This section provides a snapshot of the system’s physical storage devices and how they are organized into pools. It contains three sub-parts:  

1. **Drive Capacity Stats** – what types of drives exist in the system and their capacities.  
2. **Manual Pools** – administrator-defined pools that can host volumes.  
3. **ThinProvisioningStates** – non-manual pools tracked internally by the system for thin provisioning.  

### Drive Capacity Stats

**Description**  
Reports the **types of drives** available in the system (HDD, SSD, NVMe), along with:  

- **Drive count** for each type  
- **Current used capacity**  
- **Total raw capacity**  

**Example from log**  
```
Drives and Capacity Stats:
    84 HDD drives.  Capacity : 0.062TB / 1295.261TB
    2 SSD drives.  Capacity : 0TB / 0TB
    18 NVMe drives. Capacity : 0.068TB / 107.030TB
```

---

### Manual Pools

**Description**  
Lists the **count of manual pools** currently defined on the system and provides details for each pool instance. Each entry shows:  

- **Pool name**  
- **Number of drives in the pool**  
- **Number of volumes in the pool**  
- **Current capacity used** and **maximum capacity available**  

**Note**  
- The total number of drives across all manual pools does **not** need to match the system’s overall drive count (drives may exist outside manual pools or be reserved for other roles).  

**Example**  
```
10 Manual Pools:
    datapool : 9 drives. 6 volumes. 0.062TB / 161.908TB
    hdd1 : 9 drives. 0 volumes. 0TB / 161.908TB
    tierpool : 7 drives. 0 volumes. 0.013TB / 53.515TB
    metapool : 7 drives. 11 volumes. 0.055TB / 53.515TB
```

---

### ThinProvisioningStates (by pool)

**Description**  
Reports on **non-manual pools** used by the system for thin provisioning accounting. These pools are identified by **numeric IDs**, not by the human-readable names shown in *Manual Pools*. They track how much space can still be allocated for writes given the erasure coding scheme in use.  

Each entry reports:  

- **Pool ID** – numeric identifier of the non-manual pool.  
- **N+K** – erasure coding parameters, where `N` = data chunks and `K` = parity chunks.  
- **FreeChunks** – number of chunks available for allocation. Because writes must stripe across exactly `N+K` drives, this is capped by the least-free drive in the pool.  
- **FreePercent** – percentage of free chunks relative to the pool’s capacity.  
- **Status** – health flag (e.g., `Ok`).  

**Important Concept**  
Free space is **bounded by the smallest contributor** in the group.  
- Example: In a 4+1 pool, if 4 drives each have 10,000 free chunks but one drive only has 2,000, the system can only write 2,000 more chunks.  
- This ensures all writes can still be striped across the full redundancy set without violating the `N+K` scheme.  

**Example from log**  
```
ThinProvisioningStates (by pool):
Pool: 1190   | N+K: 3    | FreeChunks: 603,129    | FreePercent: 100.00%    - Ok
Pool: 1202   | N+K: 3    | FreeChunks: 199,251    | FreePercent: 99.95%     - Ok
```

---

## ConsistencyGroups

**Description**  
Reports the list of **consistency groups** currently tracked by the system. A consistency group is a logical grouping of volumes that must be kept in a synchronized state for correctness (e.g., crash consistency, replication ordering).  

**Example from log**  
```
Ok                     - ConsistencyGroups: [1206, 1216, 1236, 1255, 1274, 1306]
```

---

## Volumes

**Description**  
Lists all **logical volumes** currently active in the system, along with their redundancy scheme, protocol type, sector size, and geometry details. A volume is the logical unit of storage exposed to applications (block, file, or object).  

**What this section tells us**  
- **Total volume count** at the time of the snapshot.  
- **Erasure coding distribution** (e.g., “11 2+2 volumes” means all 11 volumes use a 2+2 redundancy scheme).  
- **Protocol distribution** (block vs. file/object volumes).  
- **Sector size distribution** (e.g., 4096B).  
- **Volume geometry** for each volume:  
  - Block size (I/O granularity).  
  - Maximum volume size in blocks.  
  - `UsageOutOfGeometryMax` (see caveat below).  

**Example from log**  
```
Volumes : 11

Erasure Coding Distribution:
    11 2+2 volumes

Protocol Distribution:
    1 Block volumes
    10 File/Object volumes

Sector Size Distribution:
    11 4096B volumes

Volume Geometry:
    Volume 1215 (Tiered): BlockSize=4096, MaxVolumeSizeInBlocks=137438953472, UsageOutOfGeometryMax=0.00%
```

**Caveats / Unknowns**  
- The **volume count here does not always match** the total number of volumes listed in *Manual Pools*. A reasonable hypothesis is that **metadata volumes are excluded** from this count, but this has not been confirmed.  
- The field **`UsageOutOfGeometryMax` is not fully documented**. Based on its name and context, it likely indicates space consumed beyond a volume’s defined geometry, and is expected to be `0.00%` under normal conditions. Any non-zero value should be treated as a warning.  
  - **Caveat:** This interpretation is speculative — the vendor definition is unknown.  

---

## Statistics

**Description**  
Reports instantaneous hardware utilization as percentages for CPU, disk, and memory.  

**Example from log**  
```
Hardware Utilization:
    CPU    Utilization: 4.06%
    Disk   Utilization: 0.72%
    Memory Utilization: 29.24%
```

---

## Rebuild Stats

**Description**  
Indicates whether any volumes are degraded and whether rebuild operations are in progress.  

**Example from log**  
```
Rebuild Stats
No degraded volumes & no rebuild occurring at the moment.
```

---

## HA (High Availability)

**Description**  
Reports the current **high availability state** of the system, including whether the node is part of a cluster, which node is leader, peer node status, and whether NAS services are active.  

**Example from log**  
```
HA State : All
    Node runs 11 volumes
    Is node leader : True
    Peer node status : available - True. alive - True
    Is node running NAS services : True
```

---

## Network

**Description**  
Lists the IP addresses currently assigned to the system. These may include management, client access, and replication interfaces.  

**Example from log**  
```
IP Addresses:
    192.168.100.100
    10.10.100.100
    198.51.100.1
    198.51.100.254
```

---

## Replication Status

**Description**  
Reports the state of **replication relationships** between nodes and their volumes. Each block corresponds to a replication resource (identified by a UUID) and describes the local node’s role, volume state, and replication link status.

**Fields reported**  
- **UUID** – Universally Unique Identifier for the replication resource/session. Ensures global uniqueness across clusters.  
- **node-id** – Numeric ID of a node within the cluster. Local scope (e.g., node 0, node 2).  
- **role** – Replication role of this node for the resource (`Primary`, `Secondary`).  
- **suspended** – Whether replication is administratively suspended.  
- **force-io-failures** – Policy for I/O behavior on failure.  
- **volume** – Internal volume identifier.  
- **minor** – Kernel/device minor number for mapping the device.  
- **disk** – Local disk state (`UpToDate`, `Inconsistent`, etc.).  
- **backing_dev** – Path of the local device backing the volume.  
- **quorum** – Whether this volume counts toward cluster quorum.  
- **blocked** – Whether I/O is blocked at the replication layer.  
- **connection** – Current replication link state (`Connecting`, `Established`, etc.).  
- **peer node-id** – Node ID of the peer.  
- **peer role** – Peer’s replication role (`Secondary`, `Unknown`, etc.).  
- **congested** – Whether replication link is congested.  
- **ap-in-flight** – Application writes currently in transit to peer.  
- **rs-in-flight** – Resync blocks currently in transit (replication catch-up, not local rebuild).  
- **replication** – Per-volume replication status (`On`, `Off`, `SyncTarget`, etc.).  
- **peer-disk** – Peer’s disk state (`UpToDate`, `DUnknown`, etc.).  
- **resync-suspended** – Whether resync is paused.  

**Example from log**  
```
1e7167b9-7ac1-4e90-ad4e-20d701ceff0e node-id:2 role:Primary suspended:no
    force-io-failures:no
  volume:2005 minor:2005 disk:UpToDate backing_dev:/run/s1-local/1257 quorum:yes
      blocked:no
  198.51.100.3:52002 node-id:0 connection:Connecting role:Unknown congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:2005 replication:Off peer-disk:DUnknown resync-suspended:no
```

**Reading Guide**  
- The first line shows the replication resource UUID, local node ID, this node’s role, and whether replication is suspended.  
- Each `volume` line shows the local volume state (disk state, device, quorum, blocked flag).  
- The connection line shows the peer endpoint (IP:port), peer node ID, link state, peer role, and congestion flag.  
- `ap-in-flight` and `rs-in-flight` show counts of writes or resync blocks currently in transit.  
- The final per-volume replication line shows whether replication is on/off, peer disk state, and whether resync is suspended.  

---
