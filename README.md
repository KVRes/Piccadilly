<p align="center"><img style="width: 300px" src="./doc/piccadility.png"></img></p>
<h1 align="center">Piccadilly<br>An Event-Driven High-Performance Key-Value Store</h1>

This application is a from scratch project to help me (KevinZonda) familiar with distributed systems.

## Basic Concept

A ZooKeeper-like service, but aims to provide single instance service with High Performance KV store with Event-Driven Architecture.

## Performance

PKV supports 2 write models:

- Linear (Single Thread)
- Buffer (Multi Thread, but single thread per key)

### Benchmark (w/o RPC/WAL/GC)

| Data Size | Linear                                                                                              | Buffer (KeySet=5)    |
|-----------|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| 100,000   | WR Time: 1.291293166s<br>WR Perf: 774417.48 RPS<br>RD Time: 162.476083ms<br>RD Perf: 6154752.02 RPS | WR Time: 1.153021709s<br>WR Perf: 867286.36 RPS<br>RD Time: 162.239958ms<br>RD Perf: 6163709.68 RPS |
| 500,000 | WR Time: 7.05466275s<br>WR Perf: 708751.10 RPS<br>RD Time: 973.725708ms<br>RD Perf: 5134916.29 RPS |WR Time: 6.411801333s<br>WR Perf: 779812.06 RPS<br>RD Time: 965.686375ms<br>RD Perf: 5177664.44 RPS |

