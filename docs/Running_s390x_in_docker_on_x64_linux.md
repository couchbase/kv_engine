# Running an s390x (IBM LinuxONE) Environment in Docker on x86_64 Linux

This guide walks through configuring an x86_64 Linux machine to build and run
64-bit IBM z/Architecture (`s390x`) binaries inside Docker containers via QEMU
user-mode emulation.

This setup enables building, porting, and running unit tests for software
targeting IBM LinuxONE / zSystems—such as Couchbase Server's `kv_engine` and
its dependencies—without requiring physical mainframe hardware.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Host Setup: Registering QEMU binfmt Emulation](#2-host-setup-registering-qemu-binfmt-emulation)
3. [Dockerfile and `.dockerignore`](#3-dockerfile-and-dockerignore)
4. [Building the s390x Docker Image](#4-building-the-s390x-docker-image)
5. [Starting the Container with Filesystem Mounts](#5-starting-the-container-with-filesystem-mounts)
6. [Daily Usage & Workflow](#6-daily-usage--workflow)
7. [Tips & Troubleshooting](#7-tips--troubleshooting)

---

## 1. Prerequisites

- **Host Architecture**: x86_64 (`amd64`) Linux (Ubuntu, Debian, Fedora, RHEL,
  Arch, etc.)
- **Docker**: Docker Engine 20.10+ (or Podman)
- **Host User Privileges**: User added to the `docker` group (or `sudo`
  privileges)

Verify Docker access from the host:
```bash
docker ps
```

### Assumptions

The examples in this guide assume the host user is `trond` (UID 1000), with
the home directory `/home/trond`, and the Couchbase source checked out in
`/home/trond/compile/s390x`. If your setup differs, change the username, UID
and paths in the Dockerfile (see [section 3](#3-dockerfile-and-dockerignore))
and the `docker run` command (see
[section 5.2](#52-starting-the-container)) to match.

---

## 2. Host Setup: Registering QEMU binfmt Emulation

The Linux kernel's `binfmt_misc` subsystem detects non-native binary formats
(such as s390x ELF binaries) and routes their execution through
`qemu-s390x-static`.

> [!IMPORTANT]
> **Persistence across reboots:** Writing directly to
> `/proc/sys/fs/binfmt_misc` via a one-off Docker container is stored in
> memory (`procfs`) and will **not** survive a host reboot unless persisted
> via system packages or systemd. Use **Option A** below to ensure the
> configuration automatically reloads whenever the machine reboots.

### Option A: Host System Packages (Persistent across reboots - Recommended)

Installing `qemu-user-static` on the host installs the binfmt configuration
into `/usr/lib/binfmt.d/` and enables `systemd-binfmt.service`, ensuring the
emulator is loaded automatically at every boot:

**Ubuntu / Debian:**
```bash
sudo apt-get update
sudo apt-get install -y qemu-user-static binfmt-support
```

**Fedora / RHEL / Rocky Linux:**
```bash
sudo dnf install -y qemu-user-static
sudo systemctl restart systemd-binfmt.service
```

### Option B: Using the binfmt Docker container (Quick testing / ephemeral)

If you cannot install host packages with `sudo`, you can register emulation
directly into the running kernel using Docker:

```bash
docker run --privileged --rm tonistiigi/binfmt --install s390x
```

*Note:* Because this registers into the in-memory `/proc` filesystem, this
command must be re-run after a host reboot unless you set it up as a systemd
boot service.

### Verifying Emulation

1. Check that the kernel has registered the `s390x` binary format:
   ```bash
   cat /proc/sys/fs/binfmt_misc/qemu-s390x
   ```
   Look for `enabled` and flags including `F` (fix-binary, which allows
   running inside container namespaces).

2. Test executing an s390x container:
   ```bash
   docker run --platform linux/s390x --rm ubuntu:26.04 uname -m
   ```
   The output must return:
   ```
   s390x
   ```

---

## 3. Dockerfile and `.dockerignore`

Place the following files in your workspace.

### `.dockerignore`

To prevent sending gigabytes of build artifacts and git objects as build
context to the Docker daemon, create a `.dockerignore` file:

```dockerignore
*
!Dockerfile.s390x
```

### `Dockerfile.s390x`

This Dockerfile is based on Ubuntu 26.04 (Resolute Raccoon) for
`linux/s390x`. It sets up:
- **Compilers**: `gcc-15` and `g++-15` configured via `update-alternatives` as
  the default `gcc` and `g++`.
- **Init System**: `tini` as `ENTRYPOINT` to ensure orphaned processes (such
  as interrupted `gcc` or `ninja` sub-processes) are properly reaped and do
  not linger as zombie (`<defunct>`) processes.
- **User Environment**: Creates/configures a user matching host UID `1000`
  with passwordless `sudo`, ensuring files written to mounted host
  directories maintain matching permissions.
- **Dependencies**: Common C++20/C++23 build tools (`cmake`, `ninja`,
  `ccache`, `gdb`, `python3`) and development headers (`boost`, `libevent`,
  `openssl`, `snappy`, `zlib`, `fmt`, `spdlog`, `nlohmann-json`, `jemalloc`,
  `numa`, `jq`).

```dockerfile
FROM --platform=linux/s390x ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc-15 \
    g++-15 \
    gdb \
    cmake \
    ninja-build \
    git \
    curl \
    jq \
    vim \
    golang-go \
    wget \
    ccache \
    pkg-config \
    python3 \
    python3-pip \
    python3-venv \
    sudo \
    tini \
    ca-certificates \
    libssl-dev \
    libevent-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    libboost-all-dev \
    libfmt-dev \
    libspdlog-dev \
    nlohmann-json3-dev \
    libnuma-dev \
    libjemalloc-dev \
    libdouble-conversion-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    libiberty-dev \
    libdwarf-dev \
    libunwind-dev \
    && rm -rf /var/lib/apt/lists/*

# Set gcc-15 and g++-15 as default compilers
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-15 100 \
    --slave /usr/bin/g++ g++ /usr/bin/g++-15 \
    --slave /usr/bin/gcov gcov /usr/bin/gcov-15

# Rename existing uid 1000 user or create trond
RUN if getent passwd 1000 > /dev/null 2>&1; then \
        EXISTING_USER=$(getent passwd 1000 | cut -d: -f1); \
        if [ "$EXISTING_USER" != "trond" ]; then \
            usermod -l trond -d /home/trond -m "$EXISTING_USER"; \
            groupmod -n trond "$EXISTING_USER" || true; \
        fi; \
    else \
        groupadd -g 1000 trond && \
        useradd -u 1000 -g 1000 -m -s /bin/bash trond; \
    fi && \
    echo "trond ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER trond
WORKDIR /home/trond/compile/s390x

# Run tini as PID 1 so that orphaned processes (e.g. compilers left behind
# when a build started with "docker exec" is interrupted) get reaped
# instead of lingering as zombies.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["tail", "-f", "/dev/null"]
```

> [!NOTE]
> If your host username is different from `trond` (see
> [Assumptions](#assumptions)), change the username in the
> `usermod`/`useradd` block, the `USER` line and the `WORKDIR` path to match.

---

## 4. Building the s390x Docker Image

Build the Docker image using the following command from the directory
containing `Dockerfile.s390x`:

```bash
docker build -f Dockerfile.s390x -t s390x-ubuntu-builder:latest .
```

*Note:* Because package installation is executing under CPU emulation, the
initial build takes roughly 5–10 minutes. Subsequent builds will benefit from
Docker layer caching.

---

## 5. Starting the Container with Filesystem Mounts

### 5.1 Host Directory Preparation

Building Couchbase Server downloads a fair amount of data: the build fetches
prebuilt third-party dependency packages (cbdeps) and tools, and caches them
in the user's home directory (`~/.cbdepscache` and `~/.cbdepcache`). If these
directories only exist inside the container, they disappear with it, and
every new container has to download all the packages again.

Create the cache directories on the host instead and mount them into the
container (see [5.2](#52-starting-the-container)). The downloaded packages
then survive the container being removed or recreated, and can be shared by
other s390x containers. Use dedicated s390x directories rather than the
host's own `~/.cbdepscache`, because the packages are architecture specific
and must not be mixed with the host's x86_64 packages.

Create the directories before launching the container, so Docker does not
auto-create them owned by `root`:

```bash
mkdir -p ~/.cbdepscache_s390x
mkdir -p ~/.cbdepcache_s390x
```

### 5.2 Starting the Container

Run the container with:
- **`--restart unless-stopped`**: Automatically restarts the container across
  system reboots or Docker daemon restarts.
- **Volume Mounts (`-v`)**:
  - The source code workspace directory.
  - Dedicated s390x dependency cache directories (so s390x binary artifacts
    remain separated from host x86_64 artifacts).

```bash
docker run -d \
  --name s390x-builder \
  --restart unless-stopped \
  --platform linux/s390x \
  -v /home/trond/compile/s390x:/home/trond/compile/s390x \
  -v /home/trond/.cbdepscache_s390x:/home/trond/.cbdepscache \
  -v /home/trond/.cbdepcache_s390x:/home/trond/.cbdepcache \
  s390x-ubuntu-builder:latest
```

### 5.3 Verifying Configuration

Verify that the container is running and check its mounts:

```bash
docker inspect s390x-builder --format 'Restart Policy: {{.HostConfig.RestartPolicy.Name}}
Mounts:
{{range .Mounts}}  {{.Source}} -> {{.Destination}} (RW: {{.RW}})
{{end}}
Entrypoint: {{.Config.Entrypoint}}'
```

---

## 6. Daily Usage & Workflow

### 6.1 Interactive Shell

Attach an interactive Bash shell to the running container:

```bash
docker exec -it s390x-builder bash
```

Inside the shell, verify the compiler and architecture:
```bash
uname -m          # Outputs: s390x
gcc --version     # Outputs: gcc (Ubuntu 15.2.x)
g++ --version     # Outputs: g++ (Ubuntu 15.2.x)
jq --version      # Outputs: jq-1.8.x
```

### 6.2 Running Commands Directly from the Host

You can invoke commands inside the container directly from host scripts or
terminals:

```bash
docker exec -it s390x-builder ninja -C build-s390x
```

### 6.3 Installing Additional Packages

Because the `trond` user inside the container has passwordless `sudo`, you can
install additional tools without restarting the container:

```bash
docker exec -it s390x-builder sudo apt-get update
docker exec -it s390x-builder sudo apt-get install -y <package-name>
```

Or open a root shell:
```bash
docker exec -it -u 0 s390x-builder bash
```

### 6.4 Lifecycle Commands

```bash
# Stop the container
docker stop s390x-builder

# Start the container
docker start s390x-builder

# View container logs
docker logs s390x-builder
```

---

## 7. Tips & Troubleshooting

1. **Compilation Speed & Parallelism (`-j`)**:
   - Emulating s390x instructions on an x86_64 CPU incurs a ~5x–10x slowdown
     compared to bare metal.
   - Avoid launching too many concurrent compilation processes (`-j 64`), as
     this can lead to high memory usage and QEMU CPU scheduling contention.
     Use `-j 4` to `-j 8` (e.g. `ninja -j 4`).

2. **Zombie Process Prevention with `tini`**:
   - When building via `docker exec`, terminating or interrupting a terminal
     session (`Ctrl+C`) reparents child compiler processes to PID 1 inside
     the container namespace.
   - `tini` as `ENTRYPOINT` ensures orphaned compiler jobs exit and are reaped
     properly without locking files or accumulating as zombies.

3. **Endianness Differences**:
   - Remember that s390x is a **Big-Endian** platform. Any binary files or raw
     serialized structures written to mounted volumes will be in big-endian
     byte order.
