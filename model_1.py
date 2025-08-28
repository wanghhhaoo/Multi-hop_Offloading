import random
from dataclasses import dataclass, field
from typing import List

# from collections import deque

random.seed(42)


@dataclass
class UAV:
    id: int
    # 用整数表示队列中的任务“数量”（不再存储具体任务对象）
    tx_queue: int = 0  # 传输队列中的任务数
    cp_queue: int = 0  # 计算队列中的任务数
    # 本地待处理任务：本地待传输/本地待计算（尚未进入对应队列）
    local_tx: int = 0
    local_cp: int = 0
    # 队列容量上限：
    tx_capacity: int = 20
    cp_capacity: int = 20
    # 单位时隙速率：每个时隙最多处理/传输 1 个任务
    tx_rate: int = 2
    cp_rate: int = 1
    neighbors: List[int] = field(default_factory=list)  # 环形拓扑的邻居列表（双向环）

    def enqueue_tx(self, n: int = 1):
        """向传输队列加入 n 个任务（只记录数量）"""
        available = self.tx_capacity - self.tx_queue
        added = min(n, max(0, available))
        self.tx_queue += added
        dropped = n - added
        if dropped > 0:
            print(f"UAV-{self.id} 的传输队列已满，丢弃 {dropped} 个任务")
        return added, dropped

    def dequeue_tx(self):
        """从传输队列取出 1 个任务（若有），返回 1 或 0"""
        if self.tx_queue > 0:
            self.tx_queue -= 1
            return 1
        return 0

    def enqueue_cp(self, n: int = 1):
        """向计算队列加入 n 个任务（只记录数量）"""
        available = self.cp_capacity - self.cp_queue
        added = min(n, max(0, available))
        self.cp_queue += added
        dropped = n - added
        if dropped > 0:
            print(f"UAV-{self.id} 的计算队列已满，丢弃 {dropped} 个任务")
        return added, dropped

    def dequeue_cp(self):
        """从计算队列取出 1 个任务（若有），返回 1 或 0"""
        if self.cp_queue > 0:
            self.cp_queue -= 1
            return 1
        return 0


# def generate_uavs(n=4):
#     uavs = []
#     for i in range(n):
#         # 速率设为 1：一个时隙内最多传输 1 个任务、处理 1 个任务
#         uavs.append(UAV(id=i))
#     # 构建环形（双向）拓扑：i 的邻居是 (i-1)%n 与 (i+1)%n
#     for i in range(n):
#         uavs[i].neighbors = [(i - 1) % n, (i + 1) % n]
#     return uavs


# 1) 生成 7 架无人机，采用自定义拓扑
def generate_uavs(n=7):
    uavs = [UAV(id=i) for i in range(n)]
    # 根据图定义邻居
    neighbors_map = {
        0: [1, 4],
        1: [0, 2, 4],
        2: [1, 3, 5],
        3: [2, 6],
        4: [0, 1],
        5: [2, 6],
        6: [3, 5],
    }
    for i in range(n):
        uavs[i].neighbors = neighbors_map[i]
    return uavs


# 将每个 UAV 的计算队列初始化为给定数量的任务（超出容量的会被丢弃）
def seed_initial_tasks(uavs, to: str = "cp"):
    for u in uavs:
        # 随机产生 [10, 20] 个任务，并按 1/2 划分到本地待传输与待计算池
        total = random.randint(10, 20)
        half_tx = total // 2
        half_cp = total - half_tx
        u.local_tx += half_tx
        u.local_cp += half_cp


# 按“计算队列长度最短”选择邻居（若并列，取 id 较小者）
def choose_neighbor_by_shortest_cp(uavs, u: UAV) -> UAV:
    # 如果当前 UAV 没有本地待传输任务且传输队列为空，则不需要目标节点
    if u.local_tx == 0 and u.tx_queue == 0:
        return None

    cand_ids = u.neighbors
    best = None
    for nid in cand_ids:
        v = uavs[nid]
        if best is None:
            best = v
        else:
            # 先比 cp_queue 长度，再比可用 cp 容量，再比 id
            if v.cp_queue < best.cp_queue:
                best = v
            elif v.cp_queue == best.cp_queue:
                rem_v = v.cp_capacity - v.cp_queue
                rem_b = best.cp_capacity - best.cp_queue
                if rem_v > rem_b or (rem_v == rem_b and v.id < best.id):
                    best = v
    if best is not None:
        print(f"\tUAV_{u.id} --> UAV_{best.id}")
    return best


# 按时隙推进：本地一半任务用于传输（按邻居 cp_queue 最短卸载），一半用于本地计算
# 时隙内顺序：先把本地待计算转入本地 cp_queue（受容量限制）→ 执行传输卸载（受对方容量限制）→ 本地从 cp_queue 处理
# 终止条件：所有 UAV 的 local_tx、local_cp、cp_queue 均为 0


def simulate_local_tx_and_cp(uavs, max_slots: int = 1000, verbose: bool = True):

    # 终止条件
    def all_done():
        return all(
            (
                u.local_tx == 0
                and u.local_cp == 0
                and u.tx_queue == 0
                and u.cp_queue == 0
            )
            for u in uavs
        )

    slot = 0
    while not all_done():
        if verbose:
            print(
                f"slot {slot}:\n"
                + "\n".join(
                    [
                        f"\tUAV_{u.id}: local: ({u.local_tx}, {u.local_cp}), Q: ({u.tx_queue}/{u.tx_capacity}, {u.cp_queue}/{u.cp_capacity})"
                        for u in uavs
                    ]
                )
            )

        # -------------------- (1) Admission --------------------
        for u in uavs:
            # local_cp -> cp_queue
            if u.local_cp > 0:
                can_put_cp = max(0, u.cp_capacity - u.cp_queue)
                if can_put_cp > 0:
                    moved_cp = min(can_put_cp, u.local_cp)
                    u.enqueue_cp(moved_cp)
                    u.local_cp -= moved_cp
            # local_tx -> tx_queue
            if u.local_tx > 0:
                can_put_tx = max(0, u.tx_capacity - u.tx_queue)
                if can_put_tx > 0:
                    moved_tx = min(can_put_tx, u.local_tx)
                    u.enqueue_tx(moved_tx)
                    u.local_tx -= moved_tx

        # -------------------- (2) Decision snapshot --------------------
        targets = [None] * len(uavs)
        for u in uavs:
            targets[u.id] = choose_neighbor_by_shortest_cp(uavs, u)

        # -------------------- (3) Service --------------------
        # Processing from cp_queue
        for u in uavs:
            processed = min(u.cp_rate, u.cp_queue)
            for _ in range(processed):
                u.dequeue_cp()
        # Transmission from tx_queue (collect arrivals to apply in phase 4)
        incoming_local = [0 for _ in uavs]
        for u in uavs:
            to_send = min(u.tx_rate, u.tx_queue)
            if to_send <= 0:
                continue
            tgt = targets[u.id]
            if tgt is not None:
                # 发送成功的任务在下一阶段转为对方 local_cp（不过滤容量；容量在下一时隙 Admission 再限制）
                incoming_local[tgt.id] += to_send
                # 从发送方 tx_queue 出队 to_send 个
                for _ in range(to_send):
                    u.dequeue_tx()

        # -------------------- (4) Arrival posting --------------------
        for idx, add_n in enumerate(incoming_local):
            if add_n > 0:
                uavs[idx].local_cp += add_n
                # print(
                #     f"UAV_{uavs[idx].id} 接收到来自 UAV_{u.id} 的 {add_n} 个任务，当前 local_cp: {uavs[idx].local_cp}"
                # )

        if slot >= max_slots:
            print(f"达到最大时隙数 {max_slots}，提前结束。")
            break

        slot += 1

    if verbose:
        print(f"=== 结束：总时隙 = {slot} ===\n")
    return slot


if __name__ == "__main__":
    uavs = generate_uavs(7)

    # 让每个无人机在开始时产生 9 个需要计算的任务
    seed_initial_tasks(uavs, to="cp")

    # 打印初始化后的占用情况
    print("=== 初始 UAV 队列占用（数量/容量）===")
    for u in uavs:
        print(
            f"UAV_{u.id}: local: ({u.local_tx}, {u.local_cp}), Q: ({u.tx_queue}/{u.tx_capacity}, {u.cp_queue}/{u.cp_capacity}), rate: ({u.tx_rate}, {u.cp_rate}), neighbors: {u.neighbors}"
        )

    # 开始按时隙推进（全部任务本地处理）
    total_slots = simulate_local_tx_and_cp(uavs, verbose=True)
    print(f"所有无人机任务处理完成，共用时隙：{total_slots}")
