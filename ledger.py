"""一份共享的 JSON 台账底座：inflight / pr_ledger / issue_ledger 三处的
_load/_ensure/_save/active/update/remove 曾是逐字复制，收敛到 JsonLedger 一处。

三个领域模块各自保留：模块头 docstring（讲各自领域语义）、各自的 record()（默认字段集不同）、
公开函数名与签名——内部委托一个模块级 JsonLedger 实例。键规则各异（inflight 用随机 uuid、
pr/issue 用 "pid#number"），故 key 由调用方算好后传入，本类不关心其含义。

懒加载（首次访问才读盘）+ 进程内单线程访问（asyncio），改完即原子落盘；best-effort
异常处理与原三处逐字一致。依赖 storage（原子写）这个叶子，不引入环。
"""
import json
import logging

from storage import atomic_write_json


class JsonLedger:
    def __init__(self, path_factory, label: str, log_name: str):
        """path_factory: 返回台账 json 路径的工厂（跟随 settings.store_path 动态取值——自检把它
        切到临时目录时真实 store 不被污染，与原各模块的 _path() 语义一致）；
        label: 落盘失败告警里的中文标签（在途登记簿 / PR 台账 / 工单台账，措辞差异原样保留）；
        log_name: logger 名（matrix-claude.<模块>，保持各模块日志分流不变）。"""
        self._path_factory = path_factory
        self._label = label
        self._log = logging.getLogger(log_name)
        self._data: dict[str, dict] = {}
        self._loaded = False

    def _load(self) -> None:
        self._loaded = True
        try:
            with open(self._path_factory()) as f:
                d = json.load(f)
            if isinstance(d, dict):
                self._data = {k: v for k, v in d.items() if isinstance(v, dict)}
        except (OSError, json.JSONDecodeError):
            self._data = {}

    def _save(self) -> None:
        try:
            atomic_write_json(self._path_factory(), self._data)
        except OSError as e:
            self._log.warning("%s落盘失败: %s", self._label, e)

    def _ensure(self) -> None:
        if not self._loaded:
            self._load()

    def active(self) -> list[dict]:
        """在册（未摘除）的全部条目。"""
        self._ensure()
        return list(self._data.values())

    def add(self, key: str, rec: dict) -> bool:
        """新登记一条：key 已在册返回 False（不覆盖），新登记返回 True。"""
        self._ensure()
        if key in self._data:
            return False
        self._data[key] = rec
        self._save()
        return True

    def get(self, key: str) -> dict | None:
        self._ensure()
        return self._data.get(key)

    def has(self, key: str) -> bool:
        self._ensure()
        return key in self._data

    def update(self, key: str, **fields) -> None:
        self._ensure()
        if key in self._data:
            self._data[key].update(fields)
            self._save()

    def remove(self, key: str) -> None:
        """摘除一条；不在册视作已摘除，不落盘。"""
        self._ensure()
        if self._data.pop(key, None) is not None:
            self._save()

    def clear(self) -> None:
        """清空整簿（如启动对账处理完后）。空簿不落盘。"""
        self._ensure()
        if self._data:
            self._data = {}
            self._save()

    def reset(self) -> None:
        """测试专用：清内存态并标记未加载，下次访问会重新 _load（从盘恢复），
        供用例切换 store_path 后强制重读。"""
        self._data = {}
        self._loaded = False
