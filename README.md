# Workron

A lightweight distributed job scheduler written in Go.

---

## Overview

Workron is a distributed job scheduler that allows you to submit jobs via a REST API and execute them across multiple concurrent workers. It is designed with simplicity and fault tolerance in mind.

The current implementation supports in-memory job queuing with multiple concurrent workers. Persistence, heartbeat-based failure detection, and distributed worker processes are planned for upcoming phases.

---

## Features

**Currently available**
- Submit jobs via REST API
- In-memory job queue with status tracking (`pending` → `running` → `done` / `failed`)
- Multiple concurrent workers with mutex-protected job claiming
- Basic job retry on failure
- List and query job status via API

**Planned**
- [ ] Separate scheduler and worker processes communicating over HTTP
- [ ] Heartbeat-based worker failure detection and job re-queuing
- [ ] Job persistence with SQLite / PostgreSQL
- [ ] Cron-style scheduling
- [ ] Job priority queue
- [ ] Web dashboard