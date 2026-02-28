from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from grs.contracts import ActionRequest, ActionResult
from grs.core import make_id
from grs.ui.charting import ChartAdapter, MatplotlibChartAdapter


@dataclass(slots=True)
class DebugGate:
    enabled: bool = False


class MainWindowFactory:
    def __init__(self, chart_adapter: ChartAdapter | None = None) -> None:
        self.chart_adapter = chart_adapter or MatplotlibChartAdapter()

    def create(
        self,
        action_handler: Callable[[ActionRequest], ActionResult],
        debug_gate: DebugGate,
    ):
        from PySide6.QtWidgets import (
            QHBoxLayout,
            QLabel,
            QMainWindow,
            QPushButton,
            QTabWidget,
            QTextEdit,
            QVBoxLayout,
            QWidget,
        )
        adapter = self.chart_adapter

        class MainWindow(QMainWindow):
            def __init__(self) -> None:
                super().__init__()
                self.setWindowTitle("Gridiron Rail: Sundays")
                self.resize(1300, 820)

                self._output = QTextEdit()
                self._output.setReadOnly(True)

                tabs = QTabWidget()
                tabs.addTab(self._org_tab(), "Organization")
                tabs.addTab(self._game_tab(), "Game")
                tabs.addTab(self._history_tab(), "League History")
                tabs.addTab(self._film_tab(), "Film Room")
                tabs.addTab(self._analytics_tab(), "Analytics")

                root = QWidget()
                layout = QVBoxLayout(root)
                layout.addWidget(tabs)
                layout.addWidget(self._output)
                self.setCentralWidget(root)

            def _org_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                layout.addWidget(QLabel("Roster / Cap / Draft / FA / Trades / Staff / Ownership"))
                btn = QPushButton("Advance Week")
                btn.clicked.connect(lambda: self._dispatch("advance_week", {}))
                layout.addWidget(btn)

                if debug_gate.enabled:
                    dbg = QPushButton("Debug: Reveal Ground Truth")
                    dbg.clicked.connect(lambda: self._dispatch("debug_truth", {}))
                    layout.addWidget(dbg)
                return w

            def _game_tab(self):
                w = QWidget()
                layout = QHBoxLayout(w)
                left = QVBoxLayout()
                left.addWidget(QLabel("Playcall and strategic posture controls"))
                play_btn = QPushButton("Play One Snap")
                play_btn.clicked.connect(lambda: self._dispatch("play_snap", {}))
                sim_btn = QPushButton("Sim One Drive")
                sim_btn.clicked.connect(lambda: self._dispatch("sim_drive", {}))
                left.addWidget(play_btn)
                left.addWidget(sim_btn)
                layout.addLayout(left)
                return w

            def _history_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                layout.addWidget(QLabel("Standings, awards, archives, transaction trees"))
                return w

            def _film_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                layout.addWidget(QLabel("Retained games: play list -> rep ledger -> causality chain"))
                btn = QPushButton("Load Retained Sample")
                btn.clicked.connect(lambda: self._dispatch("load_retained", {}))
                layout.addWidget(btn)
                return w

            def _analytics_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                layout.addWidget(QLabel("Export-backed analytics"))
                chart = self._make_chart()
                layout.addWidget(chart)
                return w

            def _make_chart(self):
                labels = ["W1", "W2", "W3", "W4", "W5"]
                values = [0.15, 0.18, -0.04, 0.22, 0.28]
                return adapter.create_team_trend_widget("EPA Trend", labels, values)

            def _dispatch(self, action_type: str, payload: dict):
                result = action_handler(
                    ActionRequest(
                        request_id=make_id("req"),
                        action_type=action_type,
                        payload=payload,
                        actor_team_id="USER_TEAM",
                    )
                )
                self._output.append(f"[{action_type}] {result.message}")

        window = MainWindow()
        return window


def launch_ui(action_handler: Callable[[ActionRequest], ActionResult], debug_mode: bool = False) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication([])
    factory = MainWindowFactory()
    window = factory.create(action_handler=action_handler, debug_gate=DebugGate(enabled=debug_mode))
    window.show()
    app.exec()
