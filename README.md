<p align="center"><img style="width: 300px" src="./doc/piccadility.png"></img></p>
<h1 align="center">Piccadilly<br>An Event-Driven High-Performance Key-Value Store</h1>

This application is a from scratch project to help me (KevinZonda) familiar with distributed systems.

## Basic Concept

A ZooKeeper-like service, but aims to provide single instance service with High Performance KV store with Event-Driven Architecture.

## Performance

PKV supports 2 write models:

- Linear (Single Thread)
- NoLinear (Multi Thread, the Store must support write concurrency control)

### Benchmark (w/o RPC/WAL/GC)

Tested on MacBook Air M2 (2022) 8c CPU (4P+4E), 10c GPU, 24GB RAM, 2TB SSD.

PKV benched via direct function call (WAL, GC, Flush disabled). Redis benched via `redis-benchmark -q -n 500000`

| Data Size | Linear | NoLinear                                                                                            | Redis |
|-----------|-----------|-----------------------------------------------------------------------------------------------------| --- |
| 100,000   | WR Time: 1.291293166s<br>WR Perf: 774417.48 RPS<br>RD Time: 162.476083ms<br>RD Perf: 6154752.02 RPS | WR Time: 1.114430042s<br>WR Perf: 897319.67 RPS<br>RD Time: 161.21225ms<br>RD Perf: 6203002.56 RPS  | WR Perf (SET): 138159.72 RPS<br>RD Perf (GET): 134716.42 RPS |
| 500,000 | WR Time: 7.05466275s<br>WR Perf: 708751.10 RPS<br>RD Time: 973.725708ms<br>RD Perf: 5134916.29 RPS | WR Time: 6.241516166s<br>WR Perf: 801087.41 RPS<br>RD Time: 947.381083ms<br>RD Perf: 5277707.24 RPS | WR Perf (SET): 137931.03 RPS<br>RD Perf (GET): 137438.16 RPS |