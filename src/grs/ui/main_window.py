from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from PySide6.QtCore import Qt

from grs.contracts import ActionRequest, ActionResult, ActionType, PlaybookEntry, PlayType
from grs.core import make_id
from grs.football import ResourceResolver
from grs.ui.charting import ChartAdapter, MatplotlibChartAdapter


@dataclass(slots=True)
class DebugGate:
    enabled: bool = False


class MainWindowFactory:
    def __init__(self, chart_adapter: ChartAdapter | None = None) -> None:
        self.chart_adapter = chart_adapter or MatplotlibChartAdapter()

    def create(self, action_handler: Callable[[ActionRequest], ActionResult], debug_gate: DebugGate):
        from PySide6.QtWidgets import (
            QAbstractItemView,
            QComboBox,
            QGridLayout,
            QHBoxLayout,
            QHeaderView,
            QLabel,
            QLineEdit,
            QListWidget,
            QMainWindow,
            QMessageBox,
            QPushButton,
            QSplitter,
            QSpinBox,
            QTableWidget,
            QTableWidgetItem,
            QTabWidget,
            QTextEdit,
            QVBoxLayout,
            QWidget,
        )

        resolver = ResourceResolver()
        adapter = self.chart_adapter

        class MainWindow(QMainWindow):
            def __init__(self) -> None:
                super().__init__()
                self.setWindowTitle("Gridiron Rail: Sundays")
                self.resize(1500, 920)
                self._chart_widget: QWidget | None = None
                self._film_payload: dict[str, Any] = {}
                self._game_snaps: list[dict[str, Any]] = []
                self._playbook = self._load_playbook()

                self.output = QTextEdit()
                self.output.setReadOnly(True)
                self.output.document().setMaximumBlockCount(500)

                tabs = QTabWidget()
                tabs.addTab(self._org_tab(), "Organization")
                tabs.addTab(self._game_tab(), "Game")
                tabs.addTab(self._history_tab(), "League History")
                tabs.addTab(self._film_tab(), "Film Room")
                tabs.addTab(self._analytics_tab(), "Analytics")
                if debug_gate.enabled:
                    tabs.addTab(self._dev_tab(), "Dev Tools")

                root = QWidget()
                layout = QVBoxLayout(root)
                splitter = QSplitter()
                splitter.setOrientation(Qt.Orientation.Vertical)
                splitter.addWidget(tabs)
                splitter.addWidget(self.output)
                splitter.setSizes([760, 160])
                layout.addWidget(splitter)
                self.setCentralWidget(root)
                self.statusBar().showMessage("Ready")
                clear_log = QPushButton("Clear Event Log")
                clear_log.clicked.connect(self.output.clear)
                self.statusBar().addPermanentWidget(clear_log)

                self._init_game_controls()
                self._refresh_org()
                self._refresh_standings()
                self._refresh_game_state()
                self._refresh_retained_games()
                self._refresh_analytics()
                if debug_gate.enabled:
                    self._refresh_profiles()

            def _load_playbook(self) -> dict[str, PlaybookEntry]:
                return {pid: resolver.resolve_playbook_entry(pid) for pid in resolver.playbook_ids()}

            def _dispatch(self, action: ActionType, payload: dict[str, Any], *, log: bool = True, refresh: bool = False) -> ActionResult:
                result = action_handler(ActionRequest(make_id("req"), action, payload, "T01"))
                if log:
                    state = "OK" if result.success else "FAIL"
                    self.output.append(f"[{state}] {action.value}: {result.message}")
                    if result.data:
                        text = json.dumps(result.data, default=str)
                        self.output.append(text[:1800] + ("..." if len(text) > 1800 else ""))
                self.statusBar().showMessage(
                    f"{action.value}: {'ok' if result.success else 'failed'}",
                    4000,
                )
                if not result.success and log:
                    QMessageBox.warning(self, "Action failed", result.message)
                if refresh:
                    self._refresh_org()
                    self._refresh_standings()
                    self._refresh_game_state()
                    self._refresh_retained_games()
                    self._refresh_analytics()
                return result

            def _configure_table(self, table: QTableWidget) -> None:
                table.setAlternatingRowColors(True)
                table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
                table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
                table.setSortingEnabled(True)
                header = table.horizontalHeader()
                header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
                header.setStretchLastSection(True)

            def _set_items(self, combo: QComboBox, values: list[str], preferred: str | None = None) -> None:
                current = preferred if preferred is not None else combo.currentText()
                combo.blockSignals(True)
                combo.clear()
                for value in values:
                    combo.addItem(value)
                idx = combo.findText(current) if current else -1
                if idx >= 0:
                    combo.setCurrentIndex(idx)
                combo.blockSignals(False)

            def _org_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                row = QHBoxLayout()
                refresh = QPushButton("Refresh Org")
                refresh.clicked.connect(self._refresh_org)
                advance = QPushButton("Advance Week")
                advance.clicked.connect(lambda: self._dispatch(ActionType.ADVANCE_WEEK, {}, refresh=True))
                row.addWidget(refresh)
                row.addWidget(advance)
                self.org_text = QTextEdit()
                self.org_text.setReadOnly(True)
                self.depth_table = QTableWidget(0, 3)
                self.depth_table.setHorizontalHeaderLabels(["Slot", "Player", "Priority"])
                self._configure_table(self.depth_table)
                layout.addLayout(row)
                layout.addWidget(self.org_text)
                layout.addWidget(self.depth_table)
                return w

            def _game_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                controls = QWidget()
                grid = QGridLayout(controls)
                self.play_type = QComboBox()
                for value in [pt.value for pt in PlayType]:
                    self.play_type.addItem(value)
                self.play_type.currentTextChanged.connect(self._on_play_type_changed)
                self.playbook = QComboBox()
                self.playbook.currentTextChanged.connect(self._on_playbook_selected)
                self.personnel = QComboBox()
                self.personnel.currentTextChanged.connect(self._on_personnel_changed)
                self.formation = QComboBox()
                self.offense = QComboBox()
                self.defense = QComboBox()
                self.tempo = QComboBox()
                for value in ["normal", "hurry", "chew"]:
                    self.tempo.addItem(value)
                self.aggression = QComboBox()
                for value in ["conservative", "balanced", "aggressive"]:
                    self.aggression.addItem(value)
                grid.addWidget(QLabel("Play Type"), 0, 0)
                grid.addWidget(self.play_type, 0, 1)
                grid.addWidget(QLabel("Playbook"), 0, 2)
                grid.addWidget(self.playbook, 0, 3)
                grid.addWidget(QLabel("Personnel"), 1, 0)
                grid.addWidget(self.personnel, 1, 1)
                grid.addWidget(QLabel("Formation"), 1, 2)
                grid.addWidget(self.formation, 1, 3)
                grid.addWidget(QLabel("Offense"), 2, 0)
                grid.addWidget(self.offense, 2, 1)
                grid.addWidget(QLabel("Defense"), 2, 2)
                grid.addWidget(self.defense, 2, 3)
                grid.addWidget(QLabel("Tempo"), 3, 0)
                grid.addWidget(self.tempo, 3, 1)
                grid.addWidget(QLabel("Aggression"), 3, 2)
                grid.addWidget(self.aggression, 3, 3)
                actions = QHBoxLayout()
                set_call = QPushButton("Set Playcall")
                set_call.clicked.connect(self._set_playcall)
                play = QPushButton("Play User Game")
                play.clicked.connect(lambda: self._dispatch(ActionType.PLAY_USER_GAME, {}, refresh=True))
                sim = QPushButton("Sim User Game")
                sim.clicked.connect(lambda: self._dispatch(ActionType.SIM_DRIVE, {}, refresh=True))
                refresh = QPushButton("Refresh Game")
                refresh.clicked.connect(self._refresh_game_state)
                actions.addWidget(set_call)
                actions.addWidget(play)
                actions.addWidget(sim)
                actions.addWidget(refresh)
                self.game_summary = QLabel("No game state.")
                self.game_table = QTableWidget(0, 11)
                self.game_table.setHorizontalHeaderLabels(
                    ["Play", "Type", "Terminal", "Yds", "Score", "TO", "Pen", "Reps", "Contests", "Clock", "Cond"]
                )
                self._configure_table(self.game_table)
                self.game_table.itemSelectionChanged.connect(self._render_game_play)
                self.game_detail = QTextEdit()
                self.game_detail.setReadOnly(True)
                game_split = QSplitter()
                game_split.setOrientation(Qt.Orientation.Horizontal)
                game_split.addWidget(self.game_table)
                game_split.addWidget(self.game_detail)
                game_split.setSizes([930, 450])
                layout.addWidget(controls)
                layout.addLayout(actions)
                layout.addWidget(self.game_summary)
                layout.addWidget(game_split)
                return w

            def _history_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                refresh = QPushButton("Refresh Standings")
                refresh.clicked.connect(self._refresh_standings)
                self.standings = QTableWidget(0, 5)
                self.standings.setHorizontalHeaderLabels(["Team", "W", "L", "T", "PD"])
                self._configure_table(self.standings)
                layout.addWidget(refresh)
                layout.addWidget(self.standings)
                return w

            def _film_tab(self):
                w = QWidget()
                layout = QHBoxLayout(w)
                left = QVBoxLayout()
                self.retained = QListWidget()
                self.retained.itemDoubleClicked.connect(self._load_retained)
                refresh = QPushButton("Refresh Retained")
                refresh.clicked.connect(self._refresh_retained_games)
                load = QPushButton("Load Game")
                load.clicked.connect(self._load_retained)
                left.addWidget(refresh)
                left.addWidget(self.retained)
                left.addWidget(load)
                mid = QVBoxLayout()
                self.film_filter = QLineEdit("")
                self.film_filter.setPlaceholderText("Filter plays by play id / terminal / score / turnover")
                self.film_filter.textChanged.connect(self._apply_film_filter)
                self.film_plays = QTableWidget(0, 5)
                self.film_plays.setHorizontalHeaderLabels(["Play", "Yds", "Score", "TO", "Terminal"])
                self._configure_table(self.film_plays)
                self.film_plays.itemSelectionChanged.connect(self._render_film_play)
                mid.addWidget(self.film_filter)
                mid.addWidget(self.film_plays)
                right = QVBoxLayout()
                self.film_detail = QTextEdit()
                self.film_detail.setReadOnly(True)
                right.addWidget(self.film_detail)
                layout.addLayout(left, 1)
                layout.addLayout(mid, 2)
                layout.addLayout(right, 3)
                return w

            def _analytics_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                refresh = QPushButton("Refresh Analytics")
                refresh.clicked.connect(self._refresh_analytics)
                self.analytics_host = QWidget()
                self.analytics_layout = QVBoxLayout(self.analytics_host)
                self.analytics_text = QTextEdit()
                self.analytics_text.setReadOnly(True)
                self.analytics_text.document().setMaximumBlockCount(200)
                layout.addWidget(refresh)
                layout.addWidget(self.analytics_host)
                layout.addWidget(self.analytics_text)
                return w

            def _dev_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                tools = QHBoxLayout()
                football_audit = QPushButton("Run Football Audit")
                football_audit.clicked.connect(lambda: self._dev_action(ActionType.RUN_FOOTBALL_AUDIT, {}))
                strict_audit = QPushButton("Run Strict Audit")
                strict_audit.clicked.connect(lambda: self._dev_action(ActionType.RUN_STRICT_AUDIT, {}))
                tools.addWidget(football_audit)
                tools.addWidget(strict_audit)
                profile_row = QHBoxLayout()
                self.profile = QComboBox()
                refresh = QPushButton("Refresh Profiles")
                refresh.clicked.connect(self._refresh_profiles)
                set_profile = QPushButton("Set Profile")
                set_profile.clicked.connect(lambda: self._dev_action(ActionType.SET_TUNING_PROFILE, {"profile_id": self.profile.currentText()}))
                profile_row.addWidget(self.profile)
                profile_row.addWidget(refresh)
                profile_row.addWidget(set_profile)
                patch_row = QGridLayout()
                self.family_patch = QLineEdit("{}")
                self.outcome_patch = QLineEdit("{}")
                patch = QPushButton("Patch Profile")
                patch.clicked.connect(self._patch_profile)
                patch_row.addWidget(QLabel("Family JSON"), 0, 0)
                patch_row.addWidget(self.family_patch, 0, 1)
                patch_row.addWidget(QLabel("Outcome JSON"), 1, 0)
                patch_row.addWidget(self.outcome_patch, 1, 1)
                patch_row.addWidget(patch, 1, 2)
                cal_row = QHBoxLayout()
                self.cal_play = QComboBox()
                for value in [pt.value for pt in PlayType]:
                    self.cal_play.addItem(value)
                self.cal_profile = QComboBox()
                for value in ["uniform_50", "narrow_45_55", "band_40_60"]:
                    self.cal_profile.addItem(value)
                self.cal_samples = QSpinBox()
                self.cal_samples.setRange(1, 50000)
                self.cal_samples.setValue(500)
                self.cal_seed = QLineEdit("")
                self.cal_seed.setPlaceholderText("optional integer seed")
                run = QPushButton("Run Batch")
                run.clicked.connect(self._run_batch)
                export = QPushButton("Export Calibration")
                export.clicked.connect(lambda: self._dev_action(ActionType.EXPORT_CALIBRATION_REPORT, {}))
                cal_row.addWidget(QLabel("Play"))
                cal_row.addWidget(self.cal_play)
                cal_row.addWidget(QLabel("Trait Profile"))
                cal_row.addWidget(self.cal_profile)
                cal_row.addWidget(QLabel("Samples"))
                cal_row.addWidget(self.cal_samples)
                cal_row.addWidget(QLabel("Seed"))
                cal_row.addWidget(self.cal_seed)
                cal_row.addWidget(run)
                cal_row.addWidget(export)
                self.dev_text = QTextEdit()
                self.dev_text.setReadOnly(True)
                layout.addLayout(tools)
                layout.addLayout(profile_row)
                layout.addLayout(patch_row)
                layout.addLayout(cal_row)
                layout.addWidget(self.dev_text)
                return w

            def _init_game_controls(self) -> None:
                self._on_play_type_changed(self.play_type.currentText())

            def _entries_for_play_type(self, play_type: str) -> list[PlaybookEntry]:
                return [entry for entry in self._playbook.values() if entry.play_type.value == play_type]

            def _on_play_type_changed(self, play_type: str) -> None:
                entries = self._entries_for_play_type(play_type)
                self._set_items(self.playbook, sorted([entry.play_id for entry in entries]))
                self._set_items(self.personnel, sorted(list({entry.personnel_id for entry in entries})))
                self._set_items(self.formation, sorted(list({entry.formation_id for entry in entries})))
                self._set_items(self.offense, sorted(list({entry.offensive_concept_id for entry in entries})))
                self._set_items(self.defense, sorted(list({entry.defensive_concept_id for entry in entries})))
                self._on_playbook_selected(self.playbook.currentText())

            def _on_personnel_changed(self, personnel: str) -> None:
                entries = self._entries_for_play_type(self.play_type.currentText())
                formations = sorted(list({entry.formation_id for entry in entries if entry.personnel_id == personnel}))
                if formations:
                    self._set_items(self.formation, formations)

            def _on_playbook_selected(self, play_id: str) -> None:
                if play_id not in self._playbook:
                    return
                entry = self._playbook[play_id]
                self._set_items(self.personnel, [self.personnel.itemText(i) for i in range(self.personnel.count())], preferred=entry.personnel_id)
                self._set_items(self.formation, [self.formation.itemText(i) for i in range(self.formation.count())], preferred=entry.formation_id)
                self._set_items(self.offense, [self.offense.itemText(i) for i in range(self.offense.count())], preferred=entry.offensive_concept_id)
                self._set_items(self.defense, [self.defense.itemText(i) for i in range(self.defense.count())], preferred=entry.defensive_concept_id)

            def _set_playcall(self) -> None:
                payload = {
                    "play_type": self.play_type.currentText(),
                    "personnel": self.personnel.currentText(),
                    "formation": self.formation.currentText(),
                    "offensive_concept": self.offense.currentText(),
                    "defensive_concept": self.defense.currentText(),
                    "tempo": self.tempo.currentText(),
                    "aggression": self.aggression.currentText(),
                    "playbook_entry_id": self.playbook.currentText(),
                }
                result = self._dispatch(ActionType.SET_PLAYCALL, payload)
                if result.success:
                    self.statusBar().showMessage(
                        f"Playcall set: {payload['playbook_entry_id']}",
                        4500,
                    )

            def _refresh_org(self) -> None:
                result = self._dispatch(ActionType.GET_ORG_DASHBOARD, {}, log=False)
                if not result.success:
                    result = self._dispatch(ActionType.GET_ORG_OVERVIEW, {}, log=False)
                if not result.success or not result.data:
                    self.org_text.setPlainText("No org data.")
                    return
                data = result.data
                lines = []
                if "profile" in data:
                    lines.extend(
                        [
                            f"Profile: {data['profile']['profile_name']} ({data['profile']['profile_id']})",
                            f"Mode: {data.get('mode', 'unknown')}",
                            f"Team: {data['team_name']} ({data['team_id']})",
                            f"Conference/Division: {data.get('conference_id', '')} / {data.get('division_id', '')}",
                            f"Owner: {data['owner']} | Mandate: {data['mandate']}",
                            f"Cap: {data['cap_space']} | Roster: {data['roster_size']}",
                            "",
                            "Recent Transactions:",
                        ]
                    )
                else:
                    lines.extend(
                        [
                            f"Team: {data['team_name']} ({data['team_id']})",
                            f"Owner: {data['owner']} | Mandate: {data['mandate']}",
                            f"Cap: {data['cap_space']} | Roster: {data['roster_size']}",
                            "",
                            "Recent Transactions:",
                        ]
                    )
                for tx in data.get("transactions", [])[:12]:
                    lines.append(f"- W{tx['week']} {tx['tx_type']}: {tx['summary']}")
                self.org_text.setPlainText("\n".join(lines))
                depth = data.get("depth_chart", [])
                self.depth_table.setRowCount(len(depth))
                for i, row in enumerate(depth):
                    self.depth_table.setItem(i, 0, QTableWidgetItem(str(row.get("slot_role", ""))))
                    self.depth_table.setItem(i, 1, QTableWidgetItem(str(row.get("player_id", ""))))
                    self.depth_table.setItem(i, 2, QTableWidgetItem(str(row.get("priority", ""))))

            def _refresh_game_state(self) -> None:
                result = self._dispatch(ActionType.GET_GAME_STATE, {}, log=False)
                if not result.success or not result.data:
                    self.game_summary.setText("No user game played this week.")
                    self.game_table.setRowCount(0)
                    self._game_snaps = []
                    self.game_detail.setPlainText("")
                    return
                data = result.data
                state = data["state"]
                self.game_summary.setText(
                    f"{state['game_id']} | Q{state['quarter']} {state['clock_seconds']}s | "
                    f"{state['home_team_id']} {state['home_score']} - {state['away_score']} {state['away_team_id']} | "
                    f"Poss {state['possession_team_id']} {state['down']}&{state['distance']} @ {state['yard_line']}"
                )
                snaps = data["snaps"]
                self._game_snaps = snaps
                self.game_table.setRowCount(len(snaps))
                for i, snap in enumerate(snaps):
                    values = [
                        snap["play_id"],
                        snap["play_type"],
                        snap["event"],
                        snap["yards"],
                        snap["score_event"] or "",
                        snap["turnover_type"] or "",
                        snap["penalty_count"],
                        snap["rep_count"],
                        snap["contest_count"],
                        snap["clock_delta"],
                        "Y" if snap["conditioned"] else "",
                    ]
                    for j, val in enumerate(values):
                        self.game_table.setItem(i, j, QTableWidgetItem(str(val)))
                if snaps:
                    self.game_table.selectRow(len(snaps) - 1)
                    self._render_game_play()
                self.statusBar().showMessage(
                    f"Game refreshed: {data['snap_count']} snaps / {data['action_count']} actions",
                    4000,
                )

            def _render_game_play(self) -> None:
                selected = self.game_table.selectedItems()
                if not selected:
                    return
                row_index = selected[0].row()
                if row_index < 0:
                    return
                if row_index >= len(self._game_snaps):
                    return
                snap = self._game_snaps[row_index]
                lines = []
                for key in [
                    "play_id",
                    "play_type",
                    "event",
                    "yards",
                    "score_event",
                    "turnover",
                    "turnover_type",
                    "penalty_count",
                    "rep_count",
                    "contest_count",
                    "clock_delta",
                    "conditioned",
                    "attempts",
                ]:
                    lines.append(f"{key}: {snap.get(key)}")
                self.game_detail.setPlainText("\n".join(lines))

            def _refresh_standings(self) -> None:
                result = self._dispatch(ActionType.GET_STANDINGS, {}, log=False)
                rows = result.data["standings"] if result.success else []
                self.standings.setRowCount(len(rows))
                for i, row in enumerate(rows):
                    for j, value in enumerate([row["team_id"], row["wins"], row["losses"], row["ties"], row["point_diff"]]):
                        self.standings.setItem(i, j, QTableWidgetItem(str(value)))

            def _refresh_retained_games(self) -> None:
                result = self._dispatch(ActionType.GET_RETAINED_GAMES, {}, log=False)
                self.retained.clear()
                if not result.success or not result.data:
                    return
                for game in result.data["games"]:
                    self.retained.addItem(f"{game['game_id']} (S{game['season']} W{game['week']})")

            def _load_retained(self, _item: Any = None) -> None:
                item = self.retained.currentItem()
                if item is None:
                    return
                game_id = item.text().split(" ")[0]
                result = self._dispatch(ActionType.GET_FILM_ROOM_GAME, {"game_id": game_id}, log=False)
                if not result.success or not result.data:
                    self.film_detail.setPlainText(result.message)
                    return
                self._film_payload = result.data
                terminal_by_play: dict[str, str] = {}
                for node in result.data["causality"]:
                    if node["play_id"] not in terminal_by_play:
                        terminal_by_play[node["play_id"]] = node["terminal_event"]
                plays = result.data["plays"]
                self.film_plays.setRowCount(len(plays))
                for i, play in enumerate(plays):
                    values = [play["play_id"], play["yards"], play["score_event"] or "", play["turnover_type"] or "", terminal_by_play.get(play["play_id"], "")]
                    for j, val in enumerate(values):
                        self.film_plays.setItem(i, j, QTableWidgetItem(str(val)))
                if plays:
                    self.film_plays.selectRow(0)
                    self._render_film_play()

            def _apply_film_filter(self, query: str) -> None:
                text = query.strip().lower()
                for row in range(self.film_plays.rowCount()):
                    visible = False
                    for col in range(self.film_plays.columnCount()):
                        item = self.film_plays.item(row, col)
                        if item is not None and text in item.text().lower():
                            visible = True
                            break
                    self.film_plays.setRowHidden(row, bool(text) and not visible)

            def _render_film_play(self) -> None:
                selected = self.film_plays.selectedItems()
                if not selected or not self._film_payload:
                    return
                play_id = selected[0].text()
                play = next((p for p in self._film_payload["plays"] if p["play_id"] == play_id), None)
                if play is None:
                    return
                reps = [row for row in self._film_payload["reps"] if row["play_id"] == play_id]
                contests = [row for row in self._film_payload["contests"] if row["play_id"] == play_id]
                causality = [row for row in self._film_payload["causality"] if row["play_id"] == play_id]
                lines = [
                    f"Play: {play['play_id']}",
                    f"Outcome: {play['yards']}y score={play['score_event']} turnover={play['turnover_type']}",
                    "",
                    "Contests:",
                ]
                for contest in contests:
                    lines.append(f"- {contest['phase']} {contest['family']} score={contest['score']:.4f}")
                lines.append("")
                lines.append("Reps:")
                for rep in reps:
                    lines.append(f"- {rep['phase']} {rep['rep_type']} tags={','.join(rep['context_tags'])}")
                lines.append("")
                lines.append("Causality:")
                for node in causality:
                    lines.append(f"- {node['terminal_event']} <= {node['source_id']} w={node['weight']}")
                self.film_detail.setPlainText("\n".join(lines))

            def _refresh_analytics(self) -> None:
                result = self._dispatch(ActionType.GET_ANALYTICS_SERIES, {}, log=False)
                labels = result.data["labels"] if result.success else []
                values = result.data["values"] if result.success else []
                if self._chart_widget is not None:
                    self.analytics_layout.removeWidget(self._chart_widget)
                    self._chart_widget.setParent(None)
                    self._chart_widget = None
                if labels and values:
                    self._chart_widget = adapter.create_team_trend_widget("Point Differential Trend", labels, values)
                    self.analytics_layout.addWidget(self._chart_widget)
                    self.analytics_text.setPlainText("\n".join([f"{label}: {value:.2f}" for label, value in zip(labels, values)]))
                else:
                    self.analytics_text.setPlainText("No analytics points yet.")

            def _dev_action(self, action: ActionType, payload: dict[str, Any]) -> None:
                result = self._dispatch(action, payload, log=False)
                self.dev_text.append(result.message)
                if result.data:
                    self.dev_text.append(json.dumps(result.data, default=str)[:2000])

            def _refresh_profiles(self) -> None:
                result = self._dispatch(ActionType.GET_TUNING_PROFILES, {}, log=False)
                if not result.success:
                    self.dev_text.append(result.message)
                    return
                profiles = [item["profile_id"] for item in result.data["profiles"]]
                self._set_items(self.profile, profiles, preferred=result.data["active_profile_id"])
                self.dev_text.append(f"Loaded profiles: {', '.join(profiles)}")

            def _patch_profile(self) -> None:
                try:
                    family = json.loads(self.family_patch.text())
                    outcome = json.loads(self.outcome_patch.text())
                    if not isinstance(family, dict) or not isinstance(outcome, dict):
                        raise ValueError("patch payloads must be JSON objects")
                except Exception as exc:
                    self.dev_text.append(f"Patch parse failed: {exc}")
                    return
                payload = {
                    "profile_id": self.profile.currentText(),
                    "family_weight_multipliers": family,
                    "outcome_multipliers": outcome,
                }
                self._dev_action(ActionType.PATCH_TUNING_PROFILE, payload)

            def _run_batch(self) -> None:
                payload: dict[str, Any] = {
                    "play_type": self.cal_play.currentText(),
                    "sample_count": int(self.cal_samples.value()),
                    "trait_profile": self.cal_profile.currentText(),
                }
                seed_text = self.cal_seed.text().strip()
                if seed_text:
                    try:
                        payload["seed"] = int(seed_text)
                    except ValueError:
                        self.dev_text.append("Seed must be integer.")
                        return
                self._dev_action(ActionType.RUN_CALIBRATION_BATCH, payload)

        return MainWindow()


def launch_ui(action_handler: Callable[[ActionRequest], ActionResult], debug_mode: bool = False) -> None:
    from PySide6.QtWidgets import (
        QApplication,
        QComboBox,
        QDialog,
        QFormLayout,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QPushButton,
        QSpinBox,
        QVBoxLayout,
    )

    app = QApplication([])

    def dispatch(action: ActionType, payload: dict[str, Any]) -> ActionResult:
        return action_handler(ActionRequest(make_id("req"), action, payload, "T01"))

    def parse_vector(text: str) -> list[int]:
        values = [chunk.strip() for chunk in text.split(",") if chunk.strip()]
        return [int(item) for item in values]

    def parse_matrix(text: str) -> list[list[int]]:
        rows = [row.strip() for row in text.split("|") if row.strip()]
        return [parse_vector(row) for row in rows]

    class NewFranchiseWizard(QDialog):
        def __init__(self) -> None:
            super().__init__()
            self.setWindowTitle("New Franchise Setup")
            self.resize(720, 520)
            self.created = False

            root = QVBoxLayout(self)
            form = QGridLayout()
            self.profile_name = QLineEdit("My Franchise")
            self.profile_id = QLineEdit("profile_main")
            self.conference_count = QSpinBox()
            self.conference_count.setRange(1, 4)
            self.conference_count.setValue(2)
            self.divisions_per_conf = QLineEdit("2,2")
            self.teams_per_division = QLineEdit("2,2|2,2")
            self.players_per_team = QSpinBox()
            self.players_per_team.setRange(30, 75)
            self.players_per_team.setValue(53)
            self.cap_amount = QSpinBox()
            self.cap_amount.setRange(50_000_000, 500_000_000)
            self.cap_amount.setValue(255_000_000)
            self.schedule_policy = QComboBox()
            for value in ["balanced_round_robin", "division_weighted"]:
                self.schedule_policy.addItem(value)
            self.regular_season_weeks = QSpinBox()
            self.regular_season_weeks.setRange(4, 24)
            self.regular_season_weeks.setValue(18)
            self.ruleset = QComboBox()
            self.ruleset.addItem("nfl_standard_v1")
            self.difficulty = QComboBox()
            for value in ["rookie", "pro", "all_pro", "all_madden"]:
                self.difficulty.addItem(value)
            self.difficulty.setCurrentText("pro")
            self.talent = QComboBox()
            for value in ["balanced_mid", "narrow_parity", "top_heavy", "rebuild_chaos"]:
                self.talent.addItem(value)
            self.mode = QComboBox()
            for value in ["owner", "gm", "coach"]:
                self.mode.addItem(value)
            self.team_choice = QComboBox()
            self.status = QLabel("Generate teams after entering setup values.")

            form.addWidget(QLabel("Profile Name"), 0, 0)
            form.addWidget(self.profile_name, 0, 1)
            form.addWidget(QLabel("Profile ID"), 0, 2)
            form.addWidget(self.profile_id, 0, 3)
            form.addWidget(QLabel("Conference Count"), 1, 0)
            form.addWidget(self.conference_count, 1, 1)
            form.addWidget(QLabel("Divisions / Conference"), 1, 2)
            form.addWidget(self.divisions_per_conf, 1, 3)
            form.addWidget(QLabel("Teams / Division Matrix"), 2, 0)
            form.addWidget(self.teams_per_division, 2, 1, 1, 3)
            form.addWidget(QLabel("Players / Team"), 3, 0)
            form.addWidget(self.players_per_team, 3, 1)
            form.addWidget(QLabel("Cap"), 3, 2)
            form.addWidget(self.cap_amount, 3, 3)
            form.addWidget(QLabel("Schedule Policy"), 4, 0)
            form.addWidget(self.schedule_policy, 4, 1)
            form.addWidget(QLabel("Regular Weeks"), 4, 2)
            form.addWidget(self.regular_season_weeks, 4, 3)
            form.addWidget(QLabel("Ruleset"), 5, 0)
            form.addWidget(self.ruleset, 5, 1)
            form.addWidget(QLabel("Difficulty"), 5, 2)
            form.addWidget(self.difficulty, 5, 3)
            form.addWidget(QLabel("Talent Profile"), 6, 0)
            form.addWidget(self.talent, 6, 1)
            form.addWidget(QLabel("Mode"), 6, 2)
            form.addWidget(self.mode, 6, 3)
            form.addWidget(QLabel("Takeover Team"), 7, 0)
            form.addWidget(self.team_choice, 7, 1, 1, 3)
            root.addLayout(form)
            root.addWidget(self.status)

            buttons = QHBoxLayout()
            validate_btn = QPushButton("Validate + Generate Teams")
            validate_btn.clicked.connect(self._validate)
            create_btn = QPushButton("Create Franchise Save")
            create_btn.clicked.connect(self._create)
            cancel_btn = QPushButton("Cancel")
            cancel_btn.clicked.connect(self.reject)
            buttons.addWidget(validate_btn)
            buttons.addWidget(create_btn)
            buttons.addWidget(cancel_btn)
            root.addLayout(buttons)

        def _setup_payload(self) -> dict[str, Any]:
            return {
                "conference_count": int(self.conference_count.value()),
                "divisions_per_conference": parse_vector(self.divisions_per_conf.text()),
                "teams_per_division": parse_matrix(self.teams_per_division.text()),
                "roster_policy": {
                    "players_per_team": int(self.players_per_team.value()),
                    "active_gameday_min": 22,
                    "active_gameday_max": 53,
                },
                "cap_policy": {
                    "cap_amount": int(self.cap_amount.value()),
                    "dead_money_penalty_multiplier": 1.0,
                },
                "schedule_policy": {
                    "policy_id": self.schedule_policy.currentText(),
                    "regular_season_weeks": int(self.regular_season_weeks.value()),
                },
                "ruleset_id": self.ruleset.currentText(),
                "difficulty_profile_id": self.difficulty.currentText(),
                "talent_profile_id": self.talent.currentText(),
                "user_mode": self.mode.currentText(),
                "capability_overrides": {},
                "league_format_id": "custom_flexible_v1",
                "league_format_version": "1.0.0",
            }

        def _validate(self) -> None:
            try:
                payload = self._setup_payload()
            except Exception as exc:
                self.status.setText(f"Setup parse failed: {exc}")
                return
            result = dispatch(
                ActionType.VALIDATE_LEAGUE_SETUP,
                {"profile_id": self.profile_id.text().strip(), "setup": payload},
            )
            if not result.success:
                self.status.setText(result.message)
                return
            self.team_choice.clear()
            for team_id in result.data.get("team_candidates", []):
                self.team_choice.addItem(team_id)
            if result.data.get("ok"):
                self.status.setText("Validation passed. Select takeover team and create save.")
            else:
                self.status.setText(f"Validation failed with {len(result.data.get('issues', []))} issues.")

        def _create(self) -> None:
            if self.team_choice.count() == 0:
                self.status.setText("Validate setup and generate teams first.")
                return
            profile_id = self.profile_id.text().strip()
            if not profile_id:
                self.status.setText("Profile ID is required.")
                return
            result = dispatch(
                ActionType.CREATE_NEW_FRANCHISE_SAVE,
                {
                    "profile_id": profile_id,
                    "profile_name": self.profile_name.text().strip(),
                    "selected_user_team_id": self.team_choice.currentText(),
                    "setup": self._setup_payload(),
                },
            )
            if not result.success:
                self.status.setText(result.message)
                return
            self.created = True
            self.accept()

    class ProfilePicker(QDialog):
        def __init__(self) -> None:
            super().__init__()
            self.setWindowTitle("Franchise Profiles")
            self.resize(560, 180)
            self.selected_profile: str | None = None

            root = QVBoxLayout(self)
            form = QFormLayout()
            self.profile_combo = QComboBox()
            form.addRow("Profiles", self.profile_combo)
            root.addLayout(form)
            self.status = QLabel("")
            root.addWidget(self.status)

            row = QHBoxLayout()
            refresh = QPushButton("Refresh")
            refresh.clicked.connect(self._refresh)
            create_new = QPushButton("New Franchise")
            create_new.clicked.connect(self._new_franchise)
            load = QPushButton("Load")
            load.clicked.connect(self._load)
            delete = QPushButton("Delete")
            delete.clicked.connect(self._delete)
            cancel = QPushButton("Exit")
            cancel.clicked.connect(self.reject)
            row.addWidget(refresh)
            row.addWidget(create_new)
            row.addWidget(load)
            row.addWidget(delete)
            row.addWidget(cancel)
            root.addLayout(row)
            self._refresh()

        def _refresh(self) -> None:
            result = dispatch(ActionType.LIST_PROFILES, {})
            if not result.success:
                self.status.setText(result.message)
                return
            self.profile_combo.clear()
            for item in result.data.get("profiles", []):
                self.profile_combo.addItem(f"{item['profile_name']} ({item['profile_id']})", item["profile_id"])
            self.status.setText(f"{self.profile_combo.count()} profile(s) available.")

        def _new_franchise(self) -> None:
            wizard = NewFranchiseWizard()
            if wizard.exec() == QDialog.DialogCode.Accepted and wizard.created:
                self._refresh()
                self.status.setText("Franchise save created. Select profile and load.")

        def _load(self) -> None:
            if self.profile_combo.count() == 0:
                self.status.setText("No profiles available.")
                return
            profile_id = str(self.profile_combo.currentData())
            result = dispatch(ActionType.LOAD_PROFILE, {"profile_id": profile_id})
            if not result.success:
                self.status.setText(result.message)
                return
            self.selected_profile = profile_id
            self.accept()

        def _delete(self) -> None:
            if self.profile_combo.count() == 0:
                return
            profile_id = str(self.profile_combo.currentData())
            result = dispatch(ActionType.DELETE_PROFILE, {"profile_id": profile_id})
            if not result.success:
                self.status.setText(result.message)
                return
            self._refresh()

    picker = ProfilePicker()
    if picker.exec() != QDialog.DialogCode.Accepted:
        return

    factory = MainWindowFactory()
    window = factory.create(action_handler=action_handler, debug_gate=DebugGate(enabled=debug_mode))
    window.show()
    app.exec()
