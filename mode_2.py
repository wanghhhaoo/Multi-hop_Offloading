import random
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

# from collections import deque

random.seed(42)


@dataclass
class UAV:
    id: int
    # 队列/本地池：以“任务来源 UAV id”为键、数量为值
    tx_queue: Dict[int, int] = field(default_factory=dict)  # 待传输任务（按来源计数）
    cp_queue: Dict[int, int] = field(default_factory=dict)  # 待计算任务（按来源计数）
    local_tx: Dict[int, int] = field(default_factory=dict)  # 本地待传输（尚未入队）
    local_cp: Dict[int, int] = field(default_factory=dict)  # 本地待计算（尚未入队）
    # 队列容量与服务速率
    tx_capacity: int = 20
    cp_capacity: int = 20
    tx_rate: int = 2
    cp_rate: int = 1
    neighbors: List[int] = field(default_factory=list)
    # 统计：本节点最初生成的任务数/剩余未完成数/完成时隙
    initial_tasks: int = 0
    remaining_my_tasks: int = 0
    completion_slot: Optional[int] = None

    # ---------- 工具函数 ----------
    def _sum(self, d: Dict[int, int]) -> int:
        return sum(d.values()) if d else 0

    def tx_len(self) -> int:
        return self._sum(self.tx_queue)

    def cp_len(self) -> int:
        return self._sum(self.cp_queue)

    def local_tx_len(self) -> int:
        return self._sum(self.local_tx)

    def local_cp_len(self) -> int:
        return self._sum(self.local_cp)

    # ---------- 入队/出队，带来源 ----------
    def enqueue_tx(self, origin: int, n: int = 1) -> tuple:
        available = self.tx_capacity - self.tx_len()
        added = min(n, max(0, available))
        if added:
            self.tx_queue[origin] = self.tx_queue.get(origin, 0) + added
        dropped = n - added
        if dropped > 0:
            print(f"UAV-{self.id} 的传输队列已满，丢弃 {dropped} 个任务(来源{origin})")
        return added, dropped

    def dequeue_tx(self, n: int = 1) -> List[int]:
        """从传输队列取出最多 n 个任务，返回其来源列表（长度≤n）。"""
        taken: List[int] = []
        if n <= 0 or self.tx_len() == 0:
            return taken
        # 确定性顺序：按来源 id 升序取任务
        for origin in sorted(list(self.tx_queue.keys())):
            if len(taken) >= n:
                break
            can = min(self.tx_queue[origin], n - len(taken))
            if can > 0:
                self.tx_queue[origin] -= can
                if self.tx_queue[origin] == 0:
                    del self.tx_queue[origin]
                taken.extend([origin] * can)
        return taken

    def enqueue_cp(self, origin: int, n: int = 1) -> tuple:
        available = self.cp_capacity - self.cp_len()
        added = min(n, max(0, available))
        if added:
            self.cp_queue[origin] = self.cp_queue.get(origin, 0) + added
        dropped = n - added
        if dropped > 0:
            print(f"UAV-{self.id} 的计算队列已满，丢弃 {dropped} 个任务(来源{origin})")
        return added, dropped

    def dequeue_cp(self, n: int = 1) -> List[int]:
        """从计算队列取出最多 n 个任务，返回其来源列表（长度≤n）。"""
        taken: List[int] = []
        if n <= 0 or self.cp_len() == 0:
            return taken
        for origin in sorted(list(self.cp_queue.keys())):
            if len(taken) >= n:
                break
            can = min(self.cp_queue[origin], n - len(taken))
            if can > 0:
                self.cp_queue[origin] -= can
                if self.cp_queue[origin] == 0:
                    del self.cp_queue[origin]
                taken.extend([origin] * can)
        return taken


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
        total = random.randint(10, 20)
        half_tx = total // 2
        half_cp = total - half_tx
        # 记录本节点的初始与剩余
        u.initial_tasks = total
        u.remaining_my_tasks = total
        # 本地池中按“来源=自身 id”记录
        if half_tx > 0:
            u.local_tx[u.id] = u.local_tx.get(u.id, 0) + half_tx
        if half_cp > 0:
            u.local_cp[u.id] = u.local_cp.get(u.id, 0) + half_cp


# 按“计算队列长度最短”选择邻居（若并列，取 id 较小者）
def choose_neighbor_by_shortest_cp(uavs, u: UAV) -> UAV:
    # 只有当存在本地待传输任务时才需要选择目标
    if u.local_tx_len() == 0 and u.tx_len() == 0:
        return None

    cand_ids = u.neighbors
    best = None
    for nid in cand_ids:
        v = uavs[nid]
        if best is None:
            best = v
        else:
            if v.cp_len() < best.cp_len():
                best = v
            elif v.cp_len() == best.cp_len():
                rem_v = v.cp_capacity - v.cp_len()
                rem_b = best.cp_capacity - best.cp_len()
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
                u.local_tx_len() == 0
                and u.local_cp_len() == 0
                and u.tx_len() == 0
                and u.cp_len() == 0
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
                        f"\tUAV_{u.id}: local: ({u.local_tx_len()}, {u.local_cp_len()}), Q: ({u.tx_len()}/{u.tx_capacity}, {u.cp_len()}/{u.cp_capacity}), init={u.initial_tasks}, remain={u.remaining_my_tasks}, done_at={u.completion_slot}"
                        for u in uavs
                    ]
                )
            )

        # -------------------- (1) Admission --------------------
        for u in uavs:
            # local_cp -> cp_queue
            if u.local_cp_len() > 0:
                can_put_cp = max(0, u.cp_capacity - u.cp_len())
                if can_put_cp > 0:
                    for origin in list(sorted(u.local_cp.keys())):
                        if can_put_cp <= 0:
                            break
                        move = min(u.local_cp[origin], can_put_cp)
                        if move > 0:
                            u.enqueue_cp(origin, move)
                            u.local_cp[origin] -= move
                            if u.local_cp[origin] == 0:
                                del u.local_cp[origin]
                            can_put_cp -= move
            # local_tx -> tx_queue
            if u.local_tx_len() > 0:
                can_put_tx = max(0, u.tx_capacity - u.tx_len())
                if can_put_tx > 0:
                    for origin in list(sorted(u.local_tx.keys())):
                        if can_put_tx <= 0:
                            break
                        move = min(u.local_tx[origin], can_put_tx)
                        if move > 0:
                            u.enqueue_tx(origin, move)
                            u.local_tx[origin] -= move
                            if u.local_tx[origin] == 0:
                                del u.local_tx[origin]
                            can_put_tx -= move

        # -------------------- (2) Decision snapshot --------------------
        targets = [None] * len(uavs)
        for u in uavs:
            targets[u.id] = choose_neighbor_by_shortest_cp(uavs, u)

        # -------------------- (3) Service --------------------
        # Processing from cp_queue（记录来源以结算完成者）
        for u in uavs:
            origins = u.dequeue_cp(n=u.cp_rate)
            for origin in origins:
                owner = uavs[origin]
                if owner.remaining_my_tasks > 0:
                    owner.remaining_my_tasks -= 1
                    # 在当前时隙完成了该来源的一个任务
                    if owner.remaining_my_tasks == 0 and owner.completion_slot is None:
                        owner.completion_slot = slot
        # Transmission from tx_queue
        incoming_local: List[Dict[int, int]] = [dict() for _ in uavs]
        for u in uavs:
            to_send_origins = u.dequeue_tx(n=u.tx_rate)
            if not to_send_origins:
                continue
            tgt = targets[u.id]
            if tgt is not None:
                bucket = incoming_local[tgt.id]
                for origin in to_send_origins:
                    bucket[origin] = bucket.get(origin, 0) + 1

        # -------------------- (4) Arrival posting --------------------
        for idx, odict in enumerate(incoming_local):
            if not odict:
                continue
            for origin, cnt in odict.items():
                uavs[idx].local_cp[origin] = uavs[idx].local_cp.get(origin, 0) + cnt

        if slot >= max_slots:
            print(f"达到最大时隙数 {max_slots}，提前结束。")
            break

        slot += 1

    if verbose:
        print(f"=== 结束：总时隙 = {slot} ===\n")
    print("完成时间汇总(按 UAV id):")
    for u in uavs:
        print(f"  UAV_{u.id}: 初始{u.initial_tasks}个, 完成时隙 = {u.completion_slot}")
    return slot


if __name__ == "__main__":
    uavs = generate_uavs(7)

    # 让每个无人机在开始时产生 9 个需要计算的任务
    seed_initial_tasks(uavs, to="cp")

    # 打印初始化后的占用情况
    print("=== 初始 UAV 队列占用（数量/容量）===")
    for u in uavs:
        print(
            f"UAV_{u.id}: local: ({u.local_tx_len()}, {u.local_cp_len()}), Q: ({u.tx_len()}/{u.tx_capacity}, {u.cp_len()}/{u.cp_capacity}), init={u.initial_tasks}, remain={u.remaining_my_tasks}, done_at={u.completion_slot}, rate: ({u.tx_rate}, {u.cp_rate}), neighbors: {u.neighbors}"
        )

    # 开始按时隙推进（全部任务本地处理）
    total_slots = simulate_local_tx_and_cp(uavs, verbose=True)
    print(f"所有无人机任务处理完成，共用时隙：{total_slots}")
