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

    def create(
        self,
        action_handler: Callable[[ActionRequest], ActionResult],
        debug_gate: DebugGate,
        *,
        actor_team_id: str,
    ):
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
                self.setStyleSheet(
                    """
                    QWidget {
                        font-size: 12px;
                        color: #0f1d2b;
                        background: #f3f6fb;
                    }
                    QTabWidget::pane {
                        border: 1px solid #b9c8d8;
                        background: #ffffff;
                    }
                    QTabBar::tab {
                        background: #dfe9f5;
                        color: #0f1d2b;
                        border: 1px solid #b9c8d8;
                        border-bottom: none;
                        padding: 8px 12px;
                        min-width: 110px;
                    }
                    QTabBar::tab:selected {
                        background: #ffffff;
                        font-weight: 600;
                    }
                    QTableWidget {
                        gridline-color: #d3deeb;
                        alternate-background-color: #f5f9ff;
                        background: #ffffff;
                        selection-background-color: #2f6ea7;
                        selection-color: #ffffff;
                        color: #0d1b2a;
                    }
                    QHeaderView::section {
                        background: #e7eff9;
                        color: #0f1d2b;
                        padding: 6px;
                        border: 1px solid #c7d3e1;
                        font-weight: 600;
                    }
                    QTextEdit, QLineEdit, QComboBox, QSpinBox, QListWidget {
                        background: #ffffff;
                        color: #0d1b2a;
                        border: 1px solid #c7d3e1;
                        border-radius: 4px;
                        padding: 3px;
                    }
                    QPushButton {
                        padding: 6px 11px;
                        background: #2f6ea7;
                        color: #ffffff;
                        border: 1px solid #255781;
                        border-radius: 4px;
                    }
                    QPushButton:hover { background: #265a89; }
                    QPushButton:pressed { background: #1f4b73; }
                    QLabel { color: #102030; }
                    """
                )
                self._actor_team_id = actor_team_id
                self._chart_widget: QWidget | None = None
                self._film_payload: dict[str, Any] = {}
                self._game_snaps: list[dict[str, Any]] = []
                self._schedule_rows: list[dict[str, Any]] = []
                self._playbook = self._load_playbook()

                self.output = QTextEdit()
                self.output.setReadOnly(True)
                self.output.document().setMaximumBlockCount(500)

                tabs = QTabWidget()
                tabs.addTab(self._home_tab(), "Home")
                tabs.addTab(self._team_tab(), "Team")
                tabs.addTab(self._league_tab(), "League")
                tabs.addTab(self._narrative_tab(), "Narrative")
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
                self.statusBar().showMessage(f"Ready | Team: {self._actor_team_id}")
                clear_log = QPushButton("Clear Event Log")
                clear_log.clicked.connect(self.output.clear)
                self.statusBar().addPermanentWidget(clear_log)

                self._init_game_controls()
                self._refresh_runtime_readiness()
                self._refresh_home()
                self._refresh_org()
                self._refresh_team_playbook_catalog()
                self._refresh_league_structure()
                self._refresh_schedule()
                self._refresh_standings()
                self._refresh_game_state()
                self._refresh_retained_games()
                self._refresh_analytics()
                if debug_gate.enabled:
                    self._refresh_profiles()

            def _load_playbook(self) -> dict[str, PlaybookEntry]:
                return {pid: resolver.resolve_playbook_entry(pid) for pid in resolver.playbook_ids()}

            def _dispatch(self, action: ActionType, payload: dict[str, Any], *, log: bool = True, refresh: bool = False) -> ActionResult:
                result = action_handler(ActionRequest(make_id("req"), action, payload, self._actor_team_id))
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
                    self._refresh_home()
                    self._refresh_org()
                    self._refresh_team_playbook_catalog()
                    self._refresh_league_structure()
                    self._refresh_schedule()
                    self._refresh_standings()
                    self._refresh_game_state()
                    self._refresh_retained_games()
                    self._refresh_analytics()
                return result

            def _refresh_runtime_readiness(self) -> None:
                result = self._dispatch(ActionType.GET_RUNTIME_READINESS, {}, log=False)
                if not result.success or not result.data:
                    self.statusBar().showMessage("Runtime readiness unavailable", 5000)
                    return
                checks = result.data.get("checks", {})
                ready = bool(result.data.get("ready"))
                failed = [key for key, value in checks.items() if not bool(value)]
                if ready:
                    self.statusBar().showMessage("Runtime readiness: OK", 5000)
                    return
                detail = ", ".join(failed) if failed else "unknown"
                self.statusBar().showMessage(f"Runtime readiness failed: {detail}", 10000)

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

            def _home_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                intro = QLabel(
                    "Quick Start Workflow: 1) Refresh this week 2) Select a game row "
                    "3) Set selected user game 4) Set playcall 5) Play or Sim."
                )
                intro.setWordWrap(True)
                intro.setStyleSheet(
                    "background:#eaf3ff; border:1px solid #b8cee6; padding:8px; border-radius:4px; font-weight:600;"
                )
                layout.addWidget(intro)

                top_row = QHBoxLayout()
                refresh_home = QPushButton("Refresh Dashboard")
                refresh_home.clicked.connect(self._refresh_home)
                refresh_home.setToolTip("Reload status and workflow checklist.")
                refresh_schedule = QPushButton("Refresh This Week")
                refresh_schedule.clicked.connect(self._refresh_schedule)
                refresh_schedule.setToolTip("Load schedule rows for selected week.")
                set_user_game = QPushButton("Set Selected User Game")
                set_user_game.clicked.connect(self._set_user_game)
                set_user_game.setToolTip("Mark highlighted schedule row as your active user game.")
                self.btn_set_playcall = QPushButton("Set Playcall")
                self.btn_set_playcall.clicked.connect(self._set_playcall)
                self.btn_set_playcall.setToolTip("Submit current playcall parameters to the runtime.")
                self.btn_play_game = QPushButton("Play User Game")
                self.btn_play_game.clicked.connect(self._play_user_game)
                self.btn_play_game.setToolTip("Run the selected user game through the main play flow.")
                self.btn_sim_drive = QPushButton("Sim Next Drive")
                self.btn_sim_drive.clicked.connect(self._sim_next_drive)
                self.btn_sim_drive.setToolTip("Advance the game quickly by simulating a drive.")
                top_row.addWidget(refresh_home)
                top_row.addWidget(refresh_schedule)
                top_row.addWidget(set_user_game)
                top_row.addWidget(self.btn_set_playcall)
                top_row.addWidget(self.btn_play_game)
                top_row.addWidget(self.btn_sim_drive)
                top_row.addStretch(1)
                layout.addLayout(top_row)

                self.home_quick = QTextEdit()
                self.home_quick.setReadOnly(True)
                self.home_quick.setMinimumHeight(130)
                layout.addWidget(self.home_quick)

                header_row = QHBoxLayout()
                self.current_week_label = QLabel("Current Week: -")
                self.user_game_label = QLabel("User Game: -")
                self.schedule_week = QSpinBox()
                self.schedule_week.setRange(1, 24)
                self.schedule_week.setValue(1)
                self.schedule_week.valueChanged.connect(lambda _value: self._refresh_schedule())
                header_row.addWidget(self.current_week_label)
                header_row.addSpacing(18)
                header_row.addWidget(self.user_game_label)
                header_row.addStretch(1)
                header_row.addWidget(QLabel("Week"))
                header_row.addWidget(self.schedule_week)
                layout.addLayout(header_row)

                self.schedule_table = QTableWidget(0, 5)
                self.schedule_table.setHorizontalHeaderLabels(["Game ID", "Away", "Home", "Status", "User Game"])
                self._configure_table(self.schedule_table)
                self.schedule_table.setMinimumHeight(170)
                layout.addWidget(self.schedule_table)

                self.game_context = QLabel("No selected user game for this week.")
                layout.addWidget(self.game_context)
                layout.addWidget(self._game_tab())
                return w

            def _team_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                sub = QTabWidget()
                sub.addTab(self._org_tab(), "Overview / Roster / Depth / Packages")
                sub.addTab(self._team_playbooks_tab(), "Playbooks")
                sub.addTab(self._team_schedule_analytics_tab(), "Schedule / Analytics")
                sub.addTab(self._team_finances_tab(), "Finances")
                sub.addTab(self._team_pending_actions_tab(), "Pending Actions")
                sub.addTab(self._team_trade_block_tab(), "Trade Block")
                layout.addWidget(sub)
                return w

            def _team_playbooks_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                tip = QLabel("Playbook Catalog: reference only. Use Home/Game controls to set active playcall.")
                tip.setWordWrap(True)
                tip.setStyleSheet("background:#f8fbff; border:1px solid #d2e1f0; padding:5px; border-radius:4px;")
                layout.addWidget(tip)
                self.team_playbook_table = QTableWidget(0, 6)
                self.team_playbook_table.setHorizontalHeaderLabels(
                    ["Play ID", "Type", "Personnel", "Formation", "Off Concept", "Def Concept"]
                )
                self._configure_table(self.team_playbook_table)
                layout.addWidget(self.team_playbook_table)
                return w

            def _team_schedule_analytics_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                tip = QLabel("This view focuses on your team schedule and simple performance snapshots.")
                tip.setWordWrap(True)
                tip.setStyleSheet("background:#f8fbff; border:1px solid #d2e1f0; padding:5px; border-radius:4px;")
                layout.addWidget(tip)
                self.team_schedule_table = QTableWidget(0, 5)
                self.team_schedule_table.setHorizontalHeaderLabels(["Week", "Game", "Opponent", "Location", "Status"])
                self._configure_table(self.team_schedule_table)
                self.team_schedule_table.setMinimumHeight(220)
                self.team_analytics_text = QTextEdit()
                self.team_analytics_text.setReadOnly(True)
                layout.addWidget(self.team_schedule_table)
                layout.addWidget(self.team_analytics_text)
                return w

            def _team_finances_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                tip = QLabel("Finances: cap status and policy reminders. Contract tooling will expand here.")
                tip.setWordWrap(True)
                tip.setStyleSheet("background:#f8fbff; border:1px solid #d2e1f0; padding:5px; border-radius:4px;")
                layout.addWidget(tip)
                self.finances_text = QTextEdit()
                self.finances_text.setReadOnly(True)
                layout.addWidget(self.finances_text)
                return w

            def _team_pending_actions_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                tip = QLabel("Pending Actions: priority queue of what to handle before advancing.")
                tip.setWordWrap(True)
                tip.setStyleSheet("background:#f8fbff; border:1px solid #d2e1f0; padding:5px; border-radius:4px;")
                layout.addWidget(tip)
                self.pending_actions = QListWidget()
                layout.addWidget(self.pending_actions)
                return w

            def _team_trade_block_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                self.trade_block_text = QTextEdit()
                self.trade_block_text.setReadOnly(True)
                self.trade_block_text.setPlainText(
                    "Trade block scaffolding\n\n"
                    "- List of users assets on block\n"
                    "- Incoming offers feed\n"
                    "- Quick toggle to mark/unmark players and picks\n"
                    "- Trade negotiation workspace (future)\n"
                )
                layout.addWidget(self.trade_block_text)
                return w

            def _org_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                help_text = QLabel(
                    "Team Management: keep depth chart and package assignments valid before playing. "
                    "Use Auto-Build as a fast baseline, then edit slot-by-slot."
                )
                help_text.setWordWrap(True)
                help_text.setStyleSheet("background:#f0f7ff; border:1px solid #c5d7ea; padding:6px; border-radius:4px;")
                layout.addWidget(help_text)
                row = QHBoxLayout()
                refresh = QPushButton("Refresh Team Data")
                refresh.clicked.connect(self._refresh_org)
                advance = QPushButton("Advance Week")
                advance.clicked.connect(lambda: self._dispatch(ActionType.ADVANCE_WEEK, {}, refresh=True))
                auto_pkg = QPushButton("Auto-Build Packages")
                auto_pkg.clicked.connect(self._auto_build_packages)
                validate_pkg = QPushButton("Validate Packages")
                validate_pkg.clicked.connect(self._validate_packages)
                row.addWidget(refresh)
                row.addWidget(advance)
                row.addWidget(auto_pkg)
                row.addWidget(validate_pkg)
                row.addStretch(1)
                self.org_text = QTextEdit()
                self.org_text.setReadOnly(True)
                self.org_text.setMinimumHeight(160)
                self.roster_table = QTableWidget(0, 10)
                self.roster_table.setHorizontalHeaderLabels(
                    ["Player ID", "#", "Name", "Pos", "Archetype", "Age", "Scout", "Scout Conf", "Coach", "Medical"]
                )
                self._configure_table(self.roster_table)
                self.depth_table = QTableWidget(0, 3)
                self.depth_table.setHorizontalHeaderLabels(["Slot", "Player", "Priority"])
                self._configure_table(self.depth_table)
                self.package_table = QTableWidget(0, 3)
                self.package_table.setHorizontalHeaderLabels(["Package", "Slot", "Player"])
                self._configure_table(self.package_table)

                edit_row = QGridLayout()
                self.depth_slot_edit = QComboBox()
                self.depth_player_edit = QComboBox()
                self.depth_priority_edit = QSpinBox()
                self.depth_priority_edit.setRange(1, 5)
                self.depth_priority_edit.setValue(1)
                set_depth = QPushButton("Set Depth Assignment")
                set_depth.clicked.connect(self._upsert_depth_assignment)
                self.package_id_edit = QComboBox()
                self.package_slot_edit = QComboBox()
                self.package_player_edit = QComboBox()
                set_package = QPushButton("Set Package Assignment")
                set_package.clicked.connect(self._upsert_package_assignment)

                edit_row.addWidget(QLabel("Depth Slot"), 0, 0)
                edit_row.addWidget(self.depth_slot_edit, 0, 1)
                edit_row.addWidget(QLabel("Player"), 0, 2)
                edit_row.addWidget(self.depth_player_edit, 0, 3)
                edit_row.addWidget(QLabel("Priority"), 0, 4)
                edit_row.addWidget(self.depth_priority_edit, 0, 5)
                edit_row.addWidget(set_depth, 0, 6)

                edit_row.addWidget(QLabel("Package"), 1, 0)
                edit_row.addWidget(self.package_id_edit, 1, 1)
                edit_row.addWidget(QLabel("Slot"), 1, 2)
                edit_row.addWidget(self.package_slot_edit, 1, 3)
                edit_row.addWidget(QLabel("Player"), 1, 4)
                edit_row.addWidget(self.package_player_edit, 1, 5)
                edit_row.addWidget(set_package, 1, 6)

                self.package_id_edit.currentTextChanged.connect(self._on_package_changed)

                layout.addLayout(row)
                layout.addWidget(self.org_text)
                layout.addLayout(edit_row)
                split = QSplitter()
                split.setOrientation(Qt.Orientation.Horizontal)
                split.addWidget(self.roster_table)
                split.addWidget(self.depth_table)
                split.addWidget(self.package_table)
                split.setSizes([760, 360, 420])
                layout.addWidget(split)
                return w

            def _game_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                help_text = QLabel(
                    "Game Controls: set a valid playcall, then use Play User Game for full execution "
                    "or Sim Next Drive for fast progression."
                )
                help_text.setWordWrap(True)
                help_text.setStyleSheet("background:#f0f7ff; border:1px solid #c5d7ea; padding:6px; border-radius:4px;")
                layout.addWidget(help_text)
                self.game_context = QLabel("No selected user game for this week.")
                layout.addWidget(self.game_context)
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
                set_call = QPushButton("Set Playcall (Current Params)")
                set_call.clicked.connect(self._set_playcall)
                play = QPushButton("Play User Game")
                play.clicked.connect(self._play_user_game)
                sim = QPushButton("Sim Next Drive")
                sim.clicked.connect(self._sim_next_drive)
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

            def _league_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                intro = QLabel(
                    "League Hub: standings, structure, full schedule, retained-game film room, and analytics."
                )
                intro.setWordWrap(True)
                intro.setStyleSheet("background:#f0f7ff; border:1px solid #c5d7ea; padding:6px; border-radius:4px;")
                layout.addWidget(intro)
                row = QHBoxLayout()
                refresh_standings = QPushButton("Refresh Standings")
                refresh_standings.clicked.connect(self._refresh_standings)
                refresh_schedule = QPushButton("Refresh League Schedule")
                refresh_schedule.clicked.connect(self._refresh_schedule)
                refresh_structure = QPushButton("Refresh Structure")
                refresh_structure.clicked.connect(self._refresh_league_structure)
                row.addWidget(refresh_standings)
                row.addWidget(refresh_schedule)
                row.addWidget(refresh_structure)
                row.addStretch(1)
                layout.addLayout(row)

                sub = QTabWidget()

                standings_page = QWidget()
                standings_layout = QVBoxLayout(standings_page)
                split = QSplitter()
                split.setOrientation(Qt.Orientation.Horizontal)
                self.standings = QTableWidget(0, 5)
                self.standings.setHorizontalHeaderLabels(["Team", "W", "L", "T", "PD"])
                self._configure_table(self.standings)
                split.addWidget(self.standings)
                self.league_structure = QTextEdit()
                self.league_structure.setReadOnly(True)
                split.addWidget(self.league_structure)
                split.setSizes([820, 520])
                standings_layout.addWidget(split)
                sub.addTab(standings_page, "Standings + Structure")

                schedule_page = QWidget()
                schedule_layout = QVBoxLayout(schedule_page)
                self.league_schedule_table = QTableWidget(0, 5)
                self.league_schedule_table.setHorizontalHeaderLabels(
                    ["Game ID", "Away", "Home", "Status", "User Game"]
                )
                self._configure_table(self.league_schedule_table)
                schedule_layout.addWidget(self.league_schedule_table)
                sub.addTab(schedule_page, "League Schedule")

                leaders_page = QWidget()
                leaders_layout = QVBoxLayout(leaders_page)
                self.award_leaders_text = QTextEdit()
                self.award_leaders_text.setReadOnly(True)
                self.award_leaders_text.setPlainText(
                    "Award Leaders (scaffold)\n\n"
                    "- MVP race\n"
                    "- OPOY / DPOY race\n"
                    "- Rookie races\n"
                    "- Team offense/defense leaders\n"
                )
                leaders_layout.addWidget(self.award_leaders_text)
                sub.addTab(leaders_page, "Leaders + Awards")

                sub.addTab(self._film_tab(), "Film Room (Retained)")
                sub.addTab(self._analytics_tab(), "Analytics")
                layout.addWidget(sub)
                return w

            def _narrative_tab(self):
                w = QWidget()
                layout = QVBoxLayout(w)
                title = QLabel("Narrative 2.0 Scaffold")
                title.setStyleSheet("font-weight: 600; font-size: 14px;")
                layout.addWidget(title)
                self.narrative_text = QTextEdit()
                self.narrative_text.setReadOnly(True)
                self.narrative_text.setPlainText(
                    "Future narrative surface:\n\n"
                    "- Newspapers and sports journalism\n"
                    "- Talk shows and commentary\n"
                    "- Team and league rumor feed\n"
                    "- Fan/forum discourse\n"
                    "- Staff/owner/player internal messaging\n"
                    "- Storyline timeline with evidence handles\n\n"
                    "1.0 status: event scaffolding only; generation not enabled yet."
                )
                layout.addWidget(self.narrative_text)
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
                    self._refresh_home()

            def _play_user_game(self) -> None:
                result = self._dispatch(ActionType.PLAY_USER_GAME, {}, refresh=True)
                if result.success:
                    self.statusBar().showMessage("User game completed.", 5000)

            def _sim_next_drive(self) -> None:
                result = self._dispatch(ActionType.SIM_DRIVE, {}, refresh=True)
                if result.success:
                    self.statusBar().showMessage("Simulated next drive.", 5000)

            def _auto_build_packages(self) -> None:
                result = self._dispatch(ActionType.AUTO_BUILD_PACKAGE_BOOK, {}, refresh=True)
                if result.success:
                    self.statusBar().showMessage("Package book rebuilt from depth chart", 5000)

            def _validate_packages(self) -> None:
                result = self._dispatch(ActionType.VALIDATE_TEAM_PACKAGES, {}, log=True)
                if result.success:
                    blocking = result.data.get("blocking_issues", []) if result.data else []
                    warnings = result.data.get("warning_issues", []) if result.data else []
                    self.statusBar().showMessage(
                        f"Package validation: {len(blocking)} blocking / {len(warnings)} warnings",
                        7000,
                    )

            def _upsert_depth_assignment(self) -> None:
                if not self.depth_slot_edit.currentText() or not (self.depth_player_edit.currentData() or self.depth_player_edit.currentText()):
                    QMessageBox.information(self, "Depth Chart", "Select a slot and player first.")
                    return
                payload = {
                    "slot_role": self.depth_slot_edit.currentText(),
                    "player_id": self.depth_player_edit.currentData() or self.depth_player_edit.currentText(),
                    "priority": int(self.depth_priority_edit.value()),
                    "active_flag": True,
                }
                result = self._dispatch(ActionType.UPSERT_DEPTH_CHART_ASSIGNMENT, payload, refresh=True)
                if result.success:
                    self.statusBar().showMessage("Depth chart assignment updated", 5000)

            def _upsert_package_assignment(self) -> None:
                if not self.package_id_edit.currentText() or not self.package_slot_edit.currentText():
                    QMessageBox.information(self, "Package Assignment", "Select a package and slot first.")
                    return
                payload = {
                    "package_id": self.package_id_edit.currentText(),
                    "slot_role": self.package_slot_edit.currentText(),
                    "player_id": self.package_player_edit.currentData() or self.package_player_edit.currentText(),
                }
                result = self._dispatch(ActionType.UPSERT_PACKAGE_ASSIGNMENT, payload, refresh=True)
                if result.success:
                    self.statusBar().showMessage("Package assignment updated", 5000)

            def _on_package_changed(self, package_id: str) -> None:
                rows = getattr(self, "_package_rows", [])
                slots = sorted({str(row.get("slot", "")) for row in rows if str(row.get("package_id", "")) == package_id})
                self._set_items(self.package_slot_edit, slots)

            def _refresh_home(self) -> None:
                result = self._dispatch(ActionType.GET_ORG_DASHBOARD, {}, log=False)
                if not result.success or not result.data:
                    self.home_quick.setPlainText("Home dashboard unavailable. Create/load a franchise profile.")
                    return
                data = result.data
                readiness_result = self._dispatch(ActionType.GET_RUNTIME_READINESS, {}, log=False)
                readiness_ok = bool(readiness_result.success and readiness_result.data and readiness_result.data.get("ready"))
                has_schedule = len(self._schedule_rows) > 0
                has_user_game = any(bool(row.get("is_user_game")) for row in self._schedule_rows)
                package_count = int(data.get("package_count", 0))
                packages_ready = package_count > 0
                workflow_lines = [
                    f"1) Runtime Ready: {'OK' if readiness_ok else 'PENDING'}",
                    f"2) Week Schedule Loaded: {'OK' if has_schedule else 'PENDING'}",
                    f"3) User Game Selected: {'OK' if has_user_game else 'PENDING'}",
                    f"4) Team Packages Present: {'OK' if packages_ready else 'PENDING'}",
                    "5) Set Playcall from controls below, then Play User Game or Sim Next Drive.",
                ]
                lines = [
                    f"Profile: {data.get('profile', {}).get('profile_name', '')}",
                    f"Mode: {data.get('mode', 'unknown')}",
                    f"Team: {data.get('team_name', '')} ({data.get('team_id', '')})",
                    f"Cap Space: ${int(data.get('cap_space', 0)):,}",
                    f"Roster Size: {data.get('roster_size', 0)} | Packages: {data.get('package_count', 0)}",
                    "",
                    "This Week Workflow:",
                ]
                lines.extend(workflow_lines)
                lines.extend(
                    [
                        "",
                        "Tips:",
                        "- If user game is missing: select a row in schedule table and click Set Selected User Game.",
                        "- If packages are empty: go Team -> Overview / Roster and click Auto-Build Packages.",
                    ]
                )
                self.home_quick.setPlainText("\n".join(lines))

            def _refresh_team_playbook_catalog(self) -> None:
                if not hasattr(self, "team_playbook_table"):
                    return
                entries = sorted(self._playbook.values(), key=lambda entry: entry.play_id)
                self.team_playbook_table.setRowCount(len(entries))
                for i, entry in enumerate(entries):
                    values = [
                        entry.play_id,
                        entry.play_type.value,
                        entry.personnel_id,
                        entry.formation_id,
                        entry.offensive_concept_id,
                        entry.defensive_concept_id,
                    ]
                    for j, value in enumerate(values):
                        self.team_playbook_table.setItem(i, j, QTableWidgetItem(str(value)))

            def _refresh_org(self) -> None:
                result = self._dispatch(ActionType.GET_ORG_DASHBOARD, {}, log=False)
                if not result.success:
                    result = self._dispatch(ActionType.GET_ORG_OVERVIEW, {}, log=False)
                if not result.success or not result.data:
                    self.org_text.setPlainText("No org data.")
                    self.roster_table.setRowCount(0)
                    self.depth_table.setRowCount(0)
                    self.package_table.setRowCount(0)
                    if hasattr(self, "finances_text"):
                        self.finances_text.setPlainText("No finance data.")
                    if hasattr(self, "pending_actions"):
                        self.pending_actions.clear()
                        self.pending_actions.addItem("No pending actions available.")
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
                            f"Cap: {data['cap_space']} | Roster: {data['roster_size']} | Packages: {data.get('package_count', 0)}",
                            "",
                            "Recent Transactions:",
                        ]
                    )
                else:
                    lines.extend(
                        [
                            f"Team: {data['team_name']} ({data['team_id']})",
                            f"Owner: {data['owner']} | Mandate: {data['mandate']}",
                            f"Cap: {data['cap_space']} | Roster: {data['roster_size']} | Packages: {data.get('package_count', 0)}",
                            "",
                            "Recent Transactions:",
                        ]
                    )
                for tx in data.get("transactions", [])[:12]:
                    lines.append(f"- W{tx['week']} {tx['tx_type']}: {tx['summary']}")
                self.org_text.setPlainText("\n".join(lines))
                if hasattr(self, "finances_text"):
                    cap_space = int(data.get("cap_space", 0))
                    mode = str(data.get("mode", "unknown")).upper()
                    roster_size = int(data.get("roster_size", 0))
                    package_count = int(data.get("package_count", 0))
                    finance_lines = [
                        "Team Finance Snapshot",
                        "",
                        f"Mode: {mode}",
                        f"Cap Space: ${cap_space:,}",
                        f"Roster Size: {roster_size}",
                        f"Package Count: {package_count}",
                        "",
                        "Policy Notes:",
                        "- Hard cap/roster enforcement active.",
                        "- No auto-repair or silent rescue on violations.",
                    ]
                    self.finances_text.setPlainText("\n".join(finance_lines))
                if hasattr(self, "pending_actions"):
                    self.pending_actions.clear()
                    cap_space = int(data.get("cap_space", 0))
                    if cap_space < 0:
                        self.pending_actions.addItem("BLOCKING: Team is over cap. Resolve before restricted actions.")
                    elif cap_space < 5_000_000:
                        self.pending_actions.addItem("Cap is tight (<$5M). Review contracts before advancing.")
                    else:
                        self.pending_actions.addItem("No urgent finance blockers.")
                    self.pending_actions.addItem("Review depth chart and package validation before game day.")
                    self.pending_actions.addItem("Check weekly schedule and confirm selected user game.")
                roster_result = self._dispatch(
                    ActionType.GET_TEAM_ROSTER,
                    {"team_id": data.get("team_id", self._actor_team_id)},
                    log=False,
                )
                roster_rows = roster_result.data.get("roster", []) if roster_result.success else []
                self.roster_table.setRowCount(len(roster_rows))
                player_lookup: dict[str, str] = {}
                for i, row in enumerate(roster_rows):
                    player_id = str(row.get("player_id", ""))
                    player_name = str(row.get("name", ""))
                    player_lookup[player_id] = player_name
                    values = [
                        player_id,
                        row.get("jersey_number", ""),
                        player_name,
                        row.get("position", ""),
                        row.get("archetype", ""),
                        row.get("age", ""),
                        row.get("perceived_scout_estimate", ""),
                        row.get("perceived_scout_confidence", ""),
                        row.get("perceived_coach_estimate", ""),
                        row.get("perceived_medical_estimate", ""),
                    ]
                    for j, value in enumerate(values):
                        self.roster_table.setItem(i, j, QTableWidgetItem(str(value)))

                depth = roster_result.data.get("depth_chart", []) if roster_result.success else []
                self.depth_table.setRowCount(len(depth))
                depth_slots: list[str] = []
                for i, row in enumerate(depth):
                    slot_role = str(row.get("slot_role", ""))
                    player_id = str(row.get("player_id", ""))
                    depth_slots.append(slot_role)
                    label = player_lookup.get(player_id, player_id)
                    self.depth_table.setItem(i, 0, QTableWidgetItem(slot_role))
                    self.depth_table.setItem(i, 1, QTableWidgetItem(f"{label} ({player_id})"))
                    self.depth_table.setItem(i, 2, QTableWidgetItem(str(row.get("priority", ""))))

                self.depth_slot_edit.clear()
                for slot in sorted(set(depth_slots)):
                    self.depth_slot_edit.addItem(slot)
                self.depth_player_edit.clear()
                self.package_player_edit.clear()
                for row in roster_rows:
                    player_id = str(row.get("player_id", ""))
                    label = f"#{row.get('jersey_number', '')} {row.get('name', '')} ({row.get('position', '')})"
                    self.depth_player_edit.addItem(label, player_id)
                    self.package_player_edit.addItem(label, player_id)
                if hasattr(self, "team_analytics_text"):
                    position_counts: dict[str, int] = {}
                    for row in roster_rows:
                        pos = str(row.get("position", "UNK"))
                        position_counts[pos] = position_counts.get(pos, 0) + 1
                    top_scout = sorted(
                        roster_rows,
                        key=lambda r: float(r.get("perceived_scout_estimate") or 0.0),
                        reverse=True,
                    )[:8]
                    analytics_lines = ["Team Analytics Snapshot", "", "Position Counts:"]
                    analytics_lines.extend(
                        [f"- {pos}: {count}" for pos, count in sorted(position_counts.items(), key=lambda kv: kv[0])]
                    )
                    analytics_lines.append("")
                    analytics_lines.append("Top Perceived Talent (Scout):")
                    for row in top_scout:
                        analytics_lines.append(
                            f"- {row.get('name', '')} {row.get('position', '')} "
                            f"est={row.get('perceived_scout_estimate', '')} "
                            f"conf={row.get('perceived_scout_confidence', '')}"
                        )
                    self.team_analytics_text.setPlainText("\n".join(analytics_lines))

                package_result = self._dispatch(
                    ActionType.GET_PACKAGE_BOOK,
                    {"team_id": data.get("team_id", self._actor_team_id)},
                    log=False,
                )
                assignments = package_result.data.get("assignments", {}) if package_result.success else {}
                rows: list[dict[str, str]] = []
                for package_id, mapping in dict(assignments).items():
                    for slot, player_id in dict(mapping).items():
                        rows.append({"package_id": str(package_id), "slot": str(slot), "player_id": str(player_id)})
                rows.sort(key=lambda item: (item["package_id"], item["slot"]))
                self._package_rows = rows
                self.package_table.setRowCount(len(rows))
                for i, row in enumerate(rows):
                    player_label = player_lookup.get(row["player_id"], row["player_id"])
                    self.package_table.setItem(i, 0, QTableWidgetItem(row["package_id"]))
                    self.package_table.setItem(i, 1, QTableWidgetItem(row["slot"]))
                    self.package_table.setItem(i, 2, QTableWidgetItem(f"{player_label} ({row['player_id']})"))
                package_ids = sorted({row["package_id"] for row in rows})
                self._set_items(self.package_id_edit, package_ids)
                self._on_package_changed(self.package_id_edit.currentText())

            def _refresh_league_structure(self) -> None:
                result = self._dispatch(ActionType.GET_LEAGUE_STRUCTURE, {}, log=False)
                if not result.success or not result.data:
                    self.league_structure.setPlainText("No league structure data.")
                    return
                data = result.data
                lines = [
                    f"Season {data['season']} Week {data['week']} ({data['phase']})",
                    f"Teams: {data['team_count']}",
                    "",
                ]
                for conference in data.get("conferences", []):
                    lines.append(f"{conference['conference_id']} ({conference['division_count']} divisions)")
                    for division in conference.get("divisions", []):
                        lines.append(f"  {division['division_id']} ({division['team_count']} teams)")
                        for team in division.get("teams", []):
                            lines.append(f"    - {team.get('team_name', '')} ({team.get('team_id', '')})")
                self.league_structure.setPlainText("\n".join(lines))

            def _refresh_schedule(self) -> None:
                week = int(self.schedule_week.value())
                result = self._dispatch(ActionType.GET_WEEK_SCHEDULE, {"week": week}, log=False)
                if not result.success or not result.data:
                    self.schedule_table.setRowCount(0)
                    if hasattr(self, "league_schedule_table"):
                        self.league_schedule_table.setRowCount(0)
                    if hasattr(self, "team_schedule_table"):
                        self.team_schedule_table.setRowCount(0)
                    self._schedule_rows = []
                    self.user_game_label.setText("User Game: -")
                    self.game_context.setText("No selected user game for this week.")
                    return
                data = result.data
                current_week = int(data.get("current_week", week))
                self.current_week_label.setText(f"Current Week: {current_week}")
                self._current_week = current_week
                rows = data.get("games", [])
                self._schedule_rows = rows
                self.schedule_table.setRowCount(len(rows))
                for i, row in enumerate(rows):
                    values = [
                        row.get("game_id", ""),
                        f"{row.get('away_team_name', '')} ({row.get('away_team_id', '')})",
                        f"{row.get('home_team_name', '')} ({row.get('home_team_id', '')})",
                        row.get("status", ""),
                        "YES" if bool(row.get("is_user_game")) else "",
                    ]
                    for j, value in enumerate(values):
                        self.schedule_table.setItem(i, j, QTableWidgetItem(str(value)))
                if hasattr(self, "league_schedule_table"):
                    self.league_schedule_table.setRowCount(len(rows))
                    for i, row in enumerate(rows):
                        values = [
                            row.get("game_id", ""),
                            f"{row.get('away_team_name', '')} ({row.get('away_team_id', '')})",
                            f"{row.get('home_team_name', '')} ({row.get('home_team_id', '')})",
                            row.get("status", ""),
                            "YES" if bool(row.get("is_user_game")) else "",
                        ]
                        for j, value in enumerate(values):
                            self.league_schedule_table.setItem(i, j, QTableWidgetItem(str(value)))
                if hasattr(self, "team_schedule_table"):
                    team_rows = [
                        row
                        for row in rows
                        if str(row.get("home_team_id", "")) == self._actor_team_id
                        or str(row.get("away_team_id", "")) == self._actor_team_id
                    ]
                    self.team_schedule_table.setRowCount(len(team_rows))
                    for i, row in enumerate(team_rows):
                        is_home = str(row.get("home_team_id", "")) == self._actor_team_id
                        opponent_name = (
                            row.get("away_team_name", "")
                            if is_home
                            else row.get("home_team_name", "")
                        )
                        location = "Home" if is_home else "Away"
                        values = [
                            row.get("week", week),
                            row.get("game_id", ""),
                            opponent_name,
                            location,
                            row.get("status", ""),
                        ]
                        for j, value in enumerate(values):
                            self.team_schedule_table.setItem(i, j, QTableWidgetItem(str(value)))
                self._update_user_game_context()
                self._refresh_home()

            def _set_user_game(self) -> None:
                selected = self.schedule_table.selectedItems()
                if not selected and hasattr(self, "league_schedule_table"):
                    selected = self.league_schedule_table.selectedItems()
                if not selected:
                    QMessageBox.information(self, "Select Game", "Select a game from the schedule table first.")
                    return
                table = selected[0].tableWidget()
                if table is None:
                    QMessageBox.information(self, "Select Game", "Selected row is invalid.")
                    return
                row_index = selected[0].row()
                game_item = table.item(row_index, 0)
                if game_item is None:
                    QMessageBox.information(self, "Select Game", "Selected row has no game id.")
                    return
                game_id = game_item.text().strip()
                if not game_id:
                    QMessageBox.information(self, "Select Game", "Selected row has no game id.")
                    return
                result = self._dispatch(
                    ActionType.SET_USER_GAME,
                    {"week": int(self.schedule_week.value()), "game_id": game_id},
                )
                if result.success:
                    self._refresh_schedule()

            def _update_user_game_context(self) -> None:
                user_row = next((row for row in self._schedule_rows if bool(row.get("is_user_game"))), None)
                if user_row is None:
                    text = "No selected user game for this week."
                else:
                    text = (
                        f"User Game: {user_row['game_id']} | "
                        f"{user_row['away_team_name']} ({user_row['away_team_id']}) @ "
                        f"{user_row['home_team_name']} ({user_row['home_team_id']}) | {user_row['status']}"
                    )
                self.user_game_label.setText(text)
                self.game_context.setText(text)

            def _refresh_game_state(self) -> None:
                self._update_user_game_context()
                result = self._dispatch(ActionType.GET_GAME_STATE, {}, log=False)
                if not result.success or not result.data:
                    self.game_summary.setText(
                        "No game state yet. Set this week's user game, then set playcall and run Play/Sim."
                    )
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
                if hasattr(self, "award_leaders_text"):
                    leaders = sorted(rows, key=lambda r: int(r.get("point_diff", 0)), reverse=True)[:8]
                    lines = [
                        "League Leaders Snapshot",
                        "",
                        "Top Point Differential Teams:",
                    ]
                    for row in leaders:
                        lines.append(
                            f"- {row.get('team_id', '')}: "
                            f"{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('ties', 0)} "
                            f"(PD {row.get('point_diff', 0)})"
                        )
                    lines.extend(
                        [
                            "",
                            "Awards Module (Scaffold):",
                            "- MVP, OPOY, DPOY, ROTY leaders pending deeper org metrics wiring.",
                        ]
                    )
                    self.award_leaders_text.setPlainText("\n".join(lines))

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
        QMessageBox,
        QPushButton,
        QSpinBox,
        QVBoxLayout,
    )

    app = QApplication([])

    class Context:
        actor_team_id = "T01"

    def dispatch(action: ActionType, payload: dict[str, Any]) -> ActionResult:
        return action_handler(ActionRequest(make_id("req"), action, payload, Context.actor_team_id))

    class NewFranchiseWizard(QDialog):
        PRESETS: dict[str, tuple[int, int, int, int, str]] = {
            "Heritage/Frontier 32 (Default)": (2, 4, 4, 18, "heritage_frontier_32_v1"),
            "Custom Flexible": (2, 2, 4, 18, "generated_custom_v1"),
            "Compact 8-Team": (2, 2, 2, 14, "generated_custom_v1"),
            "Balanced 16-Team": (2, 2, 4, 16, "generated_custom_v1"),
        }

        def __init__(self) -> None:
            super().__init__()
            self.setWindowTitle("New Franchise Setup")
            self.resize(800, 580)
            self.created = False
            root = QVBoxLayout(self)
            form = QGridLayout()
            self.profile_name = QLineEdit("My Franchise")
            self.profile_id = QLineEdit("profile_main")
            self.preset = QComboBox()
            for preset in self.PRESETS:
                self.preset.addItem(preset)
            self.preset.currentTextChanged.connect(self._apply_preset)
            self.conference_count = QSpinBox()
            self.conference_count.setRange(1, 4)
            self.conference_count.setValue(2)
            self.divisions_per_conf = QSpinBox()
            self.divisions_per_conf.setRange(1, 8)
            self.divisions_per_conf.setValue(2)
            self.teams_per_division = QSpinBox()
            self.teams_per_division.setRange(2, 16)
            self.teams_per_division.setValue(2)
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
            self.identity_profile = QComboBox()
            self.identity_profile.addItem("Heritage/Frontier 32", "heritage_frontier_32_v1")
            self.identity_profile.addItem("Generated Name Bank", "generated_custom_v1")
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
            self.total = QLabel("")
            self.team_choice = QComboBox()
            self.status = QLabel("Pick preset/inputs, validate, then choose takeover team.")

            form.addWidget(QLabel("Profile Name"), 0, 0)
            form.addWidget(self.profile_name, 0, 1)
            form.addWidget(QLabel("Profile ID"), 0, 2)
            form.addWidget(self.profile_id, 0, 3)
            form.addWidget(QLabel("Preset"), 1, 0)
            form.addWidget(self.preset, 1, 1)
            form.addWidget(QLabel("Conferences"), 1, 2)
            form.addWidget(self.conference_count, 1, 3)
            form.addWidget(QLabel("Divisions/Conference"), 2, 0)
            form.addWidget(self.divisions_per_conf, 2, 1)
            form.addWidget(QLabel("Teams/Division"), 2, 2)
            form.addWidget(self.teams_per_division, 2, 3)
            form.addWidget(QLabel("Players/Team"), 3, 0)
            form.addWidget(self.players_per_team, 3, 1)
            form.addWidget(QLabel("Cap"), 3, 2)
            form.addWidget(self.cap_amount, 3, 3)
            form.addWidget(QLabel("Schedule"), 4, 0)
            form.addWidget(self.schedule_policy, 4, 1)
            form.addWidget(QLabel("Regular Weeks"), 4, 2)
            form.addWidget(self.regular_season_weeks, 4, 3)
            form.addWidget(QLabel("Ruleset"), 5, 0)
            form.addWidget(self.ruleset, 5, 1)
            form.addWidget(QLabel("League Identity"), 5, 2)
            form.addWidget(self.identity_profile, 5, 3)
            form.addWidget(QLabel("Difficulty"), 6, 0)
            form.addWidget(self.difficulty, 6, 1)
            form.addWidget(QLabel("Talent"), 6, 2)
            form.addWidget(self.talent, 6, 3)
            form.addWidget(QLabel("Mode"), 7, 0)
            form.addWidget(self.mode, 7, 1)
            form.addWidget(QLabel("Topology"), 7, 2)
            form.addWidget(self.total, 7, 3)
            form.addWidget(QLabel("Takeover Team"), 8, 0)
            form.addWidget(self.team_choice, 8, 1, 1, 3)
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

            self.conference_count.valueChanged.connect(self._update_total)
            self.divisions_per_conf.valueChanged.connect(self._update_total)
            self.teams_per_division.valueChanged.connect(self._update_total)
            self._apply_preset(self.preset.currentText())
            self._update_total()

        def _apply_preset(self, preset: str) -> None:
            conf, divs, teams, weeks, identity_profile_id = self.PRESETS[preset]
            self.conference_count.setValue(conf)
            self.divisions_per_conf.setValue(divs)
            self.teams_per_division.setValue(teams)
            self.regular_season_weeks.setValue(weeks)
            idx = self.identity_profile.findData(identity_profile_id)
            if idx >= 0:
                self.identity_profile.setCurrentIndex(idx)
            fixed_topology = identity_profile_id == "heritage_frontier_32_v1"
            self.conference_count.setEnabled(not fixed_topology)
            self.divisions_per_conf.setEnabled(not fixed_topology)
            self.teams_per_division.setEnabled(not fixed_topology)

        def _update_total(self) -> None:
            conf = int(self.conference_count.value())
            divs = int(self.divisions_per_conf.value())
            teams = int(self.teams_per_division.value())
            self.total.setText(f"{conf} x {divs} x {teams} = {conf * divs * teams} total teams")

        def _setup_payload(self) -> dict[str, Any]:
            conf = int(self.conference_count.value())
            divs = int(self.divisions_per_conf.value())
            teams = int(self.teams_per_division.value())
            return {
                "conference_count": conf,
                "divisions_per_conference": [divs for _ in range(conf)],
                "teams_per_division": [[teams for _ in range(divs)] for _ in range(conf)],
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
                "league_identity_profile_id": str(self.identity_profile.currentData()),
                "user_mode": self.mode.currentText(),
                "capability_overrides": {},
                "league_format_id": "custom_flexible_v1",
                "league_format_version": "1.0.0",
            }

        def _validate(self) -> None:
            profile_id = self.profile_id.text().strip()
            if not profile_id:
                self.status.setText("Profile ID is required.")
                return
            result = dispatch(
                ActionType.VALIDATE_LEAGUE_SETUP,
                {"profile_id": profile_id, "setup": self._setup_payload()},
            )
            if not result.success:
                self.status.setText(result.message)
                return
            self.team_choice.clear()
            for team in result.data.get("team_options", []):
                label = (
                    f"{team.get('team_name', '')} "
                    f"({team.get('team_id', '')}) | "
                    f"{team.get('conference_name', '')} {team.get('division_name', '')}"
                )
                self.team_choice.addItem(label, team.get("team_id", ""))
            if result.data.get("ok"):
                self.status.setText("Validation passed. Choose your takeover team.")
                return
            issues = result.data.get("issues", [])
            issue_lines = [f"{issue.get('code', '')}: {issue.get('message', '')}" for issue in issues]
            self.status.setText("Validation failed:\n" + "\n".join(issue_lines[:6]))

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
                    "selected_user_team_id": str(self.team_choice.currentData()),
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
            self.resize(600, 200)
            self.selected_team_id: str = "T01"

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
            self.selected_team_id = str(result.data.get("user_team_id", "T01"))
            self.accept()

        def _delete(self) -> None:
            if self.profile_combo.count() == 0:
                return
            profile_id = str(self.profile_combo.currentData())
            if QMessageBox.question(self, "Delete Profile", f"Delete profile '{profile_id}'?") != QMessageBox.StandardButton.Yes:
                return
            result = dispatch(ActionType.DELETE_PROFILE, {"profile_id": profile_id})
            if not result.success:
                self.status.setText(result.message)
                return
            self._refresh()

    picker = ProfilePicker()
    if picker.exec() != QDialog.DialogCode.Accepted:
        return

    Context.actor_team_id = picker.selected_team_id
    factory = MainWindowFactory()
    window = factory.create(
        action_handler=action_handler,
        debug_gate=DebugGate(enabled=debug_mode),
        actor_team_id=picker.selected_team_id,
    )
    window.show()
    app.exec()
