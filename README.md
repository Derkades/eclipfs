# EclipFS
EclipFS is a distributed file system built for WAN

## Design goals and features
* WAN connections are slow (only 5mbit/s upload is fine). Replication should not consume too much bandwidth and only happen with no other filesystem activity. No unnecessary rebalancing of data like other LAN distributed filesystems do.
* Chunkservers (the part of the network that stores data) should be able to disappear without affecting data integrity and uptime
* The filesystem should store checksums to verify data has not been altered on disk, accidentally or intentionally.
* For ease of use, allow secure operation over the internet without a VPN.
* No fine grained user-based permissions system to allow for simpler code with better performance. All clients can access all data. Global write access is configurable per user.
