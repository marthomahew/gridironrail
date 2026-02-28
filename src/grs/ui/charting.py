from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from PySide6.QtWidgets import QWidget


class ChartAdapter(Protocol):
    def create_team_trend_widget(self, title: str, x: list[str], y: list[float]) -> QWidget: ...


@dataclass(slots=True)
class MatplotlibChartAdapter:
    """Matplotlib-in-Qt adapter; swappable behind ChartAdapter contract."""

    def create_team_trend_widget(self, title: str, x: list[str], y: list[float]) -> QWidget:
        from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
        from matplotlib.figure import Figure

        fig = Figure(figsize=(4.5, 2.4), dpi=100)
        ax = fig.add_subplot(111)
        ax.plot(x, y, marker="o", linewidth=1.8)
        ax.set_title(title)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        canvas = FigureCanvasQTAgg(fig)
        return cast("QWidget", canvas)
