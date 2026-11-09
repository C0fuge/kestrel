"""Summarises jcmd Thread.print dumps: where the busy threads of a server are (item 39 (c), lane 4).

Usage: thread-dump-summary.py <dump file> ...

Drops idle threads (pool workers waiting for a task, Netty / gRPC event loops in epoll, timers,
reference handlers) and counts the remaining threads by state and their top frames (the first
frames up to the first frame of the server's own package, at most 8). Prints the 25 most common
stacks with their count over all dumps, most common first.
"""
import collections
import re
import sys

IDLE = (
    "ThreadPoolExecutor.getTask", "ScheduledThreadPoolExecutor$DelayedWorkQueue.take",
    "EPoll.wait", "Epoll.epollWait", "epollWait", "SelectorImpl.select", "ForkJoinPool.awaitWork",
    "Reference$ReferenceHandler", "Reference.waitForReferencePendingList", "ReferenceQueue.remove",
    "Signal Dispatcher", "LinkedBlockingQueue.take", "SynchronousQueue", "Object.wait(java.base@",
    "DelayQueue.take", "Thread.sleep", "TimerThread.mainLoop", "KQueue",
)
OWN = "pro.surpin."


def threads(text):
    """Yields (name, state, frames) of each thread of a dump."""
    for block in text.split("\n\n"):
        lines = block.strip().splitlines()
        if not lines or not lines[0].startswith('"'):
            continue
        name = lines[0].split('"')[1]
        state = ""
        frames = []
        for ln in lines[1:]:
            ln = ln.strip()
            if ln.startswith("java.lang.Thread.State:"):
                state = ln.split(":", 1)[1].strip().split(" ")[0]
            elif ln.startswith("at "):
                frames.append(re.sub(r"\(.*\)$", "", ln[3:]))
            elif ln.startswith("- waiting to lock") or ln.startswith("- parking to wait for"):
                frames.append(re.sub(r"<0x[0-9a-f]+> ", "", ln))
        yield name, state, frames, block


def idle(name, frames, block):
    """Whether a thread waits for work rather than for a query."""
    if not frames:
        return True
    head = "\n".join(frames[:6])
    if any(k in head for k in IDLE) and not any(f.startswith(OWN) for f in frames[:12]):
        return True
    return name in ("Signal Dispatcher", "Reference Handler", "Finalizer", "Common-Cleaner",
                    "Attach Listener", "Notification Thread")


def key(state, frames):
    """Top frames down to the server's first own frame, at most 8."""
    top = []
    for f in frames:
        top.append(f)
        if f.startswith(OWN) or len(top) >= 8:
            break
    return state + "\n    " + "\n    ".join(top)


def main():
    counts = collections.Counter()
    states = collections.Counter()
    busy = total = 0
    for path in sys.argv[1:]:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
        for name, state, frames, block in threads(text):
            total += 1
            if idle(name, frames, block):
                continue
            busy += 1
            states[state] += 1
            counts[key(state, frames)] += 1
    print(f"dumps={len(sys.argv) - 1} threads={total} nonIdle={busy} states={dict(states)}")
    for stack, n in counts.most_common(25):
        print(f"{n:5d} {stack}")


if __name__ == "__main__":
    main()
