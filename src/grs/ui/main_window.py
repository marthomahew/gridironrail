from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from grs.contracts import ActionRequest, ActionResult, ActionType
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
            QComboBox,
            QHBoxLayout,
            QLabel,
            QListWidget,
            QMainWindow,
            QPushButton,
            QTableWidget,
            QTableWidgetItem,
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
                self.resize(1400, 860)
                self._chart_widget: QWidget | None = None

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

                self._refresh_org()
                self._refresh_standings()
                self._refresh_retained_games()
                self._refresh_analytics()

            def _org_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                self.org_view = QTextEdit()
                self.org_view.setReadOnly(True)

                btn_row = QHBoxLayout()
                refresh_btn = QPushButton("Refresh Org")
                refresh_btn.clicked.connect(self._refresh_org)
                advance_btn = QPushButton("Advance Week")
                advance_btn.clicked.connect(lambda: self._dispatch(ActionType.ADVANCE_WEEK, {}, refresh=True))
                btn_row.addWidget(refresh_btn)
                btn_row.addWidget(advance_btn)
                layout.addLayout(btn_row)

                if debug_gate.enabled:
                    dbg = QPushButton("Debug: Reveal Ground Truth")
                    dbg.clicked.connect(lambda: self._dispatch(ActionType.DEBUG_TRUTH, {}))
                    layout.addWidget(dbg)

                layout.addWidget(self.org_view)
                return w

            def _game_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)

                playcall_row = QHBoxLayout()
                self.play_type = QComboBox()
                for value in ["run", "pass", "punt", "field_goal", "extra_point", "two_point", "kickoff"]:
                    self.play_type.addItem(value)
                self.formation = QComboBox()
                for value in ["gun_trips", "singleback", "i_form", "goal_line"]:
                    self.formation.addItem(value)

                set_call = QPushButton("Set Playcall")
                set_call.clicked.connect(self._set_playcall)
                play_game = QPushButton("Play User Game")
                play_game.clicked.connect(lambda: self._dispatch(ActionType.PLAY_USER_GAME, {}, refresh=True))
                sim_game = QPushButton("Sim User Game")
                sim_game.clicked.connect(lambda: self._dispatch(ActionType.SIM_DRIVE, {}, refresh=True))

                playcall_row.addWidget(QLabel("Play Type"))
                playcall_row.addWidget(self.play_type)
                playcall_row.addWidget(QLabel("Formation"))
                playcall_row.addWidget(self.formation)
                playcall_row.addWidget(set_call)
                playcall_row.addWidget(play_game)
                playcall_row.addWidget(sim_game)

                self.game_state_view = QTextEdit()
                self.game_state_view.setReadOnly(True)
                refresh_state = QPushButton("Refresh Game State")
                refresh_state.clicked.connect(self._refresh_game_state)

                layout.addLayout(playcall_row)
                layout.addWidget(refresh_state)
                layout.addWidget(self.game_state_view)
                return w

            def _history_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                self.standings_table = QTableWidget(0, 5)
                self.standings_table.setHorizontalHeaderLabels(["Team", "W", "L", "T", "PD"])
                refresh = QPushButton("Refresh Standings")
                refresh.clicked.connect(self._refresh_standings)
                layout.addWidget(refresh)
                layout.addWidget(self.standings_table)
                return w

            def _film_tab(self):
                w = QWidget()
                layout = QHBoxLayout(w)

                left = QVBoxLayout()
                self.retained_list = QListWidget()
                refresh = QPushButton("Refresh Retained Games")
                refresh.clicked.connect(self._refresh_retained_games)
                load = QPushButton("Load Selected")
                load.clicked.connect(self._load_selected_retained)
                left.addWidget(refresh)
                left.addWidget(self.retained_list)
                left.addWidget(load)

                right = QVBoxLayout()
                self.film_text = QTextEdit()
                self.film_text.setReadOnly(True)
                right.addWidget(self.film_text)

                layout.addLayout(left, 1)
                layout.addLayout(right, 2)
                return w

            def _analytics_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                btn = QPushButton("Refresh Analytics")
                btn.clicked.connect(self._refresh_analytics)
                layout.addWidget(btn)
                self.analytics_host = QWidget()
                self.analytics_layout = QVBoxLayout(self.analytics_host)
                layout.addWidget(self.analytics_host)
                return w

            def _dispatch(self, action_type: ActionType, payload: dict, refresh: bool = False):
                result = action_handler(
                    ActionRequest(
                        request_id=make_id("req"),
                        action_type=action_type,
                        payload=payload,
                        actor_team_id="T01",
                    )
                )
                self._output.append(f"[{action_type.value}] {result.message}")
                if result.data:
                    self._output.append(str(result.data)[:1200])
                if refresh:
                    self._refresh_org()
                    self._refresh_standings()
                    self._refresh_game_state()
                    self._refresh_retained_games()
                    self._refresh_analytics()
                return result

            def _set_playcall(self):
                payload = {
                    "play_type": self.play_type.currentText(),
                    "formation": self.formation.currentText(),
                    "personnel": "11",
                    "offensive_concept": "spacing" if self.play_type.currentText() == "pass" else "inside_zone",
                    "defensive_concept": "cover3_match",
                    "tempo": "normal",
                    "aggression": "balanced",
                }
                self._dispatch(ActionType.SET_PLAYCALL, payload)

            def _refresh_org(self):
                result = self._dispatch(ActionType.GET_ORG_OVERVIEW, {})
                data = result.data or {}
                lines = [
                    f"Team: {data.get('team_name')} ({data.get('team_id')})",
                    f"Owner: {data.get('owner')} | Mandate: {data.get('mandate')}",
                    f"Cap Space: {data.get('cap_space')} | Roster Size: {data.get('roster_size')}",
                    "",
                    "Recent Transactions:",
                ]
                for tx in data.get("transactions", [])[:10]:
                    lines.append(f"- W{tx['week']} {tx['tx_type']}: {tx['summary']}")
                self.org_view.setPlainText("\n".join(lines))

            def _refresh_game_state(self):
                result = self._dispatch(ActionType.GET_GAME_STATE, {})
                data = result.data or {}
                if not data:
                    self.game_state_view.setPlainText("No user game played this week.")
                    return
                state = data.get("state", {})
                lines = [
                    f"Game: {state.get('game_id')} Q{state.get('quarter')} {state.get('clock_seconds')}s",
                    f"Score: {state.get('home_team_id')} {state.get('home_score')} - {state.get('away_score')} {state.get('away_team_id')}",
                    f"Possession: {state.get('possession_team_id')} | {state.get('down')} & {state.get('distance')} @ {state.get('yard_line')}",
                    "",
                    "Recent Snaps:",
                ]
                for snap in data.get("snaps", [])[-20:]:
                    lines.append(f"- {snap['play_id']}: {snap['yards']}y ({snap['event']})")
                self.game_state_view.setPlainText("\n".join(lines))

            def _refresh_standings(self):
                result = self._dispatch(ActionType.GET_STANDINGS, {})
                rows = result.data.get("standings", []) if result.data else []
                self.standings_table.setRowCount(len(rows))
                for i, row in enumerate(rows):
                    values = [row["team_id"], row["wins"], row["losses"], row["ties"], row["point_diff"]]
                    for j, val in enumerate(values):
                        self.standings_table.setItem(i, j, QTableWidgetItem(str(val)))

            def _refresh_retained_games(self):
                result = self._dispatch(ActionType.GET_RETAINED_GAMES, {})
                self.retained_list.clear()
                for g in result.data.get("games", []) if result.data else []:
                    self.retained_list.addItem(f"{g['game_id']} (S{g['season']} W{g['week']})")

            def _load_selected_retained(self):
                item = self.retained_list.currentItem()
                if not item:
                    return
                game_id = item.text().split(" ")[0]
                result = self._dispatch(ActionType.GET_FILM_ROOM_GAME, {"game_id": game_id})
                data = result.data or {}
                lines = [f"Film Room: {game_id}", "", "Plays:"]
                for play in data.get("plays", [])[:30]:
                    lines.append(
                        f"- {play['play_id']}: {play['yards']}y score={play['score_event']} to={play['turnover_type']}"
                    )
                lines.append("\nCausality:")
                for c in data.get("causality", [])[:30]:
                    lines.append(f"- {c['terminal_event']} <- {c['source_id']} ({c['weight']})")
                self.film_text.setPlainText("\n".join(lines))

            def _refresh_analytics(self):
                result = self._dispatch(ActionType.GET_ANALYTICS_SERIES, {})
                labels = result.data.get("labels", []) if result.data else []
                values = result.data.get("values", []) if result.data else []
                if self._chart_widget is not None:
                    self.analytics_layout.removeWidget(self._chart_widget)
                    self._chart_widget.setParent(None)
                if labels and values:
                    self._chart_widget = adapter.create_team_trend_widget("Point Differential Trend", labels, values)
                    self.analytics_layout.addWidget(self._chart_widget)

        return MainWindow()


def launch_ui(action_handler: Callable[[ActionRequest], ActionResult], debug_mode: bool = False) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication([])
    factory = MainWindowFactory()
    window = factory.create(action_handler=action_handler, debug_gate=DebugGate(enabled=debug_mode))
    window.show()
    app.exec()
