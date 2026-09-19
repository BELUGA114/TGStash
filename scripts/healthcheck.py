"""
读心跳文件判断 stash-listener 是否还活着，供 docker compose healthcheck 调用。

进程若卡在不崩溃的状态（flood-wait 循环、代理半死），Docker 不会重启、用户也无信号。
listener 每轮循环无条件写一次心跳（epoch 秒）；本脚本读它，与当前时间比阈值，
超过则退出码非 0，Docker 据此重启容器。

阈值默认 SCAN_INTERVAL_SECONDS × 3（容三轮抖动）。心跳文件不存在（刚启动还没写过）
算 unhealthy，配合 compose 的 start_period 宽限启动期。

纯读文件：不开 DB、不连 Telegram。
"""

import os
import sys

# 与 listener.HEARTBEAT_PATH 同一约定。healthcheck 是独立进程，不 import listener
# （那会连带触发它的模块级导入），只共享这条路径约定
DATA_DIR = os.environ.get("DATA_DIR", "/data")
HEARTBEAT_PATH = os.path.join(DATA_DIR, "tmp", "heartbeat")

# 容忍多少轮扫描没写心跳。三轮：偶发一轮长扫描或慢网络不误杀
STALE_MULTIPLIER = 3


def read_heartbeat(path: str) -> float | None:
    """读心跳时间戳。文件不存在或内容不是数字都返回 None（视为 unhealthy）。"""
    try:
        with open(path, encoding="utf-8") as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return None


def is_stale(heartbeat_ts: float, now: float, threshold: float) -> bool:
    """心跳距今是否超过阈值。正好等于阈值算过期。"""
    return now - heartbeat_ts >= threshold


def main():
    import time

    scan_interval = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
    threshold = scan_interval * STALE_MULTIPLIER

    ts = read_heartbeat(HEARTBEAT_PATH)
    if ts is None:
        print(f"unhealthy: 心跳文件缺失或不可读 {HEARTBEAT_PATH}")
        sys.exit(1)

    now = time.time()
    if is_stale(ts, now, threshold):
        age = int(now - ts)
        print(f"unhealthy: 心跳过期 {age}s（阈值 {threshold}s）")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
