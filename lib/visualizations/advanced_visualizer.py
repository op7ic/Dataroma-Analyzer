#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Advanced Visualizer

Creates sophisticated multi-panel visualizations for manager performance analysis.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

"""
Visualization module for advanced manager analysis.
Creates graphs for manager performance, patterns, and strategic insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, List
import logging
from matplotlib.ticker import MaxNLocator, FuncFormatter

logger = logging.getLogger(__name__)


class AdvancedVisualizer:
    """Creates visualizations for advanced manager analysis."""

    def __init__(self, output_dir: str = "analysis/advanced/visuals"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        plt.style.use("seaborn-v0_8-darkgrid")
        sns.set_palette("deep")

        plt.rcParams["figure.dpi"] = 150
        plt.rcParams["savefig.dpi"] = 300

    def _extract_analysis_period(self, results: Dict[str, pd.DataFrame]) -> str:
        """Extract the analysis period from manager track records or other data."""
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            df = results["manager_track_records"]
            if "first_year" in df.columns and "last_year" in df.columns:
                first_year = int(df["first_year"].min())
                last_year = int(df["last_year"].max())
                total_years = last_year - first_year + 1
                return f"{first_year}-{last_year}, {total_years} years"

        for key, df in results.items():
            if df.empty:
                continue
            if "years_active" in df.columns:
                avg_years = df["years_active"].mean()
                return f"Average {avg_years:.1f} year careers"

        return "Long-term Analysis"

    @staticmethod
    def _short_name(name: str, limit: int = 18) -> str:
        """Shorten a label without silently chopping it mid-word into a fake name."""
        name = str(name).strip()
        return name if len(name) <= limit else name[: limit - 1].rstrip() + "\u2026"

    # Candidate label placements, in preference order, as (dx, dy) point offsets.
    _LABEL_OFFSETS = [
        (5, 5), (5, -5), (-5, 5), (-5, -5),
        (10, 0), (-10, 0), (0, 9), (0, -9),
        (14, 9), (-14, 9), (14, -9), (-14, -9),
    ]

    def _annotate_without_overlap(self, ax, xs, ys, labels, fontsize=8, avoid=None, **text_kw):
        """Label scatter points with greedy collision avoidance.

        Each label tries a ring of candidate offsets and keeps the first that
        neither overlaps an already-placed label (or a bbox in ``avoid``, e.g.
        a legend) nor spills outside the axes. If every candidate collides,
        the least-bad one wins. This replaces fixed nudges, which cannot keep
        co-located tickers apart or keep edge labels inside the figure.
        """
        from matplotlib.transforms import Bbox

        fig = ax.figure
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        placed = list(avoid or [])

        def overlap_area(a, b):
            box = Bbox.intersection(a, b)
            if box is None:
                return 0.0
            return max(box.width, 0.0) * max(box.height, 0.0)

        for x, y, label in zip(xs, ys, labels):
            if pd.isna(x) or pd.isna(y) or not str(label).strip():
                continue
            ann = ax.annotate(str(label), (x, y), textcoords="offset points", fontsize=fontsize, **text_kw)
            ax_bbox = ax.get_window_extent(renderer=renderer)
            best = None
            for dx, dy in self._LABEL_OFFSETS:
                ann.set_ha("left" if dx >= 0 else "right")
                ann.set_va("bottom" if dy >= 0 else "top")
                ann.xyann = (dx, dy)
                bbox = ann.get_window_extent(renderer=renderer)
                cost = sum(overlap_area(bbox, other) for other in placed)
                # Anything outside the axes is worse than any in-axes overlap.
                inside = overlap_area(bbox, ax_bbox)
                cost += (bbox.width * bbox.height - inside) * 10.0
                if cost <= 0.0:
                    best = (cost, dx, dy)
                    break
                if best is None or cost < best[0]:
                    best = (cost, dx, dy)
            _, dx, dy = best
            ann.set_ha("left" if dx >= 0 else "right")
            ann.set_va("bottom" if dy >= 0 else "top")
            ann.xyann = (dx, dy)
            placed.append(ann.get_window_extent(renderer=renderer))

    @staticmethod
    def _fold_thin_wedge_pct(wedges, texts, autotexts, min_angle=18.0):
        """Fold the percentage of a too-thin pie wedge into its outside label.

        A wedge only a few degrees wide has no room for an autopct label, so
        matplotlib prints it across the neighbouring slice boundary.
        """
        for wedge, text, auto in zip(wedges, texts, autotexts):
            if wedge.theta2 - wedge.theta1 < min_angle and auto.get_text():
                text.set_text(f"{text.get_text()}\n{auto.get_text()}")
                auto.set_text("")

    def _get_manager_name(self, row: pd.Series) -> str:
        """Get the full manager name, preferring the descriptive name over ID."""
        name_cols = ["manager_name", "manager.1", "manager_full_name"]
        id_cols = ["manager", "manager_id"]

        for col in name_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()

        for col in id_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()

        return "Unknown Manager"

    def create_all_visualizations(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Create all advanced analysis visualizations."""
        viz_paths = []

        try:
            if "manager_track_records" in results and not results["manager_track_records"].empty:
                path = self.create_manager_performance_chart(results["manager_track_records"])
                if path:
                    viz_paths.append(path)

            if "crisis_alpha_generators" in results and not results["crisis_alpha_generators"].empty:
                path = self.create_crisis_alpha_chart(results["crisis_alpha_generators"])
                if path:
                    viz_paths.append(path)

            if "position_sizing_mastery" in results and not results["position_sizing_mastery"].empty:
                path = self.create_position_sizing_chart(results["position_sizing_mastery"])
                if path:
                    viz_paths.append(path)

            if "manager_evolution_patterns" in results and not results["manager_evolution_patterns"].empty:
                path = self.create_evolution_chart(results["manager_evolution_patterns"])
                if path:
                    viz_paths.append(path)

            if "multi_manager_favorites" in results and not results["multi_manager_favorites"].empty:
                path = self.create_consensus_picks_chart(results["multi_manager_favorites"])
                if path:
                    viz_paths.append(path)

            if "top_holdings" in results and not results["top_holdings"].empty:
                path = self.create_top_holdings_chart(results["top_holdings"])
                if path:
                    viz_paths.append(path)

            # Create accumulation vs distribution visualization
            if "accumulation_vs_distribution" in results and not results["accumulation_vs_distribution"].empty:
                path = self.create_accumulation_distribution_chart(results["accumulation_vs_distribution"])
                if path:
                    viz_paths.append(path)

        except Exception as e:
            logger.error(f"Error creating advanced visualizations: {e}")

        return viz_paths

    def create_manager_performance_chart(self, df: pd.DataFrame) -> str:
        """Create comprehensive manager performance visualization."""
        try:
            fig = plt.figure(figsize=(16, 12))

            gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
            ax1 = fig.add_subplot(gs[0, :2])
            ax2 = fig.add_subplot(gs[0, 2])
            ax3 = fig.add_subplot(gs[1, :2])
            ax4 = fig.add_subplot(gs[1, 2])
            ax5 = fig.add_subplot(gs[2, :])

            top_managers = df.nlargest(15, "track_record_score")

            manager_col = "manager_name" if "manager_name" in top_managers.columns else "manager"
            _ = ax1.barh(top_managers[manager_col], top_managers["track_record_score"], color="darkblue", alpha=0.7)
            ax1.set_xlabel("Track Record Score", fontweight="bold")
            ax1.set_title("Top 15 Managers by Track Record Score", fontsize=12, fontweight="bold")
            ax1.invert_yaxis()
            ax1.grid(True, alpha=0.3)

            max_score = top_managers["track_record_score"].max()
            ax1.set_xlim(0, max_score * 1.1)  # Add padding for labels
            for i, (_, row) in enumerate(top_managers.iterrows()):
                score = row["track_record_score"]
                ax1.text(
                    score + max_score * 0.02, i, f"{score:.1f}", va="center", ha="left", fontsize=9, fontweight="bold"
                )

            # Bins must cover the whole range: include_lowest keeps 0-year managers
            # and an open upper bin keeps >20-year managers from silently vanishing
            # while the pie still claims to add up to 100%.
            bin_labels = ["0-5 years", "6-10 years", "11-15 years", "16-20 years", "20+ years"]
            years_bins = pd.cut(
                df["years_active"],
                bins=[0, 5, 10, 15, 20, float("inf")],
                labels=bin_labels,
                include_lowest=True,
            )
            years_counts = years_bins.value_counts().reindex(bin_labels).fillna(0).astype(int)
            years_counts = years_counts[years_counts > 0]
            ax2.pie(years_counts.values, labels=years_counts.index, autopct="%1.1f%%")
            ax2.set_title(f"Manager Experience Distribution\n({int(years_counts.sum())} managers)")

            # Dataroma serves at most ~1000 activity rows per manager, so
            # total_actions saturates at 1000 for the most active managers.
            # Those points are a floor, not a count - mark them as such.
            history_cap = 1000
            capped = top_managers["total_actions"] >= history_cap
            ax3.scatter(
                top_managers.loc[~capped, "total_actions"],
                top_managers.loc[~capped, "consistency_score"],
                s=100,
                alpha=0.6,
                c=top_managers.loc[~capped, "years_active"],
                cmap="viridis",
                label="Full history",
            )
            if capped.any():
                ax3.scatter(
                    top_managers.loc[capped, "total_actions"],
                    top_managers.loc[capped, "consistency_score"],
                    s=120,
                    alpha=0.8,
                    marker=">",
                    c=top_managers.loc[capped, "years_active"],
                    cmap="viridis",
                    edgecolors="red",
                    linewidth=1.2,
                    label=f"\u2265{history_cap} (history truncated)",
                )
                ax3.axvline(x=history_cap, color="red", linestyle="--", alpha=0.5, linewidth=1.5)

            # Capped managers sit exactly on the 1000 line, so without extra
            # room their labels are drawn in the figure gutter. Give the axis
            # headroom past the cap, and a band of empty space at the bottom
            # for the legend so it cannot cover a plotted manager.
            x_max = max(float(top_managers["total_actions"].max()), float(history_cap))
            x_min = float(top_managers["total_actions"].min())
            x_pad = max((x_max - x_min) * 0.12, x_max * 0.06)
            ax3.set_xlim(max(0.0, x_min - x_pad), x_max + x_pad * 2.2)

            y_vals = top_managers["consistency_score"]
            y_min, y_max = float(y_vals.min()), float(y_vals.max())
            y_pad = max((y_max - y_min) * 0.1, 0.01)
            ax3.set_ylim(y_min - y_pad * 3.0, y_max + y_pad)

            ax3.set_xlabel(f"Recorded Actions (scrape-capped at {history_cap})")
            ax3.set_ylabel("Consistency Score")
            ax3.set_title("Recorded Activity vs Consistency")

            avoid = []
            if capped.any():
                legend = ax3.legend(loc="lower left", fontsize=8, frameon=True)
                ax3.figure.canvas.draw()
                avoid.append(legend.get_window_extent(renderer=ax3.figure.canvas.get_renderer()))

            self._annotate_without_overlap(
                ax3,
                top_managers["total_actions"],
                top_managers["consistency_score"],
                [self._short_name(row[manager_col]) for _, row in top_managers.iterrows()],
                fontsize=8,
                avoid=avoid,
                alpha=0.7,
            )

            if "current_portfolio_value" in df.columns:
                top_by_value = df.nlargest(10, "current_portfolio_value")
                values_billions = top_by_value["current_portfolio_value"] / 1e9
                _ = ax4.bar(range(len(top_by_value)), values_billions, color="green", alpha=0.7)
                ax4.set_xticks(range(len(top_by_value)))
                ax4.set_xticklabels(
                    [self._short_name(n, 14) for n in top_by_value[manager_col]],
                    rotation=45,
                    ha="right",
                    fontsize=8,
                )
                ax4.set_ylabel("Portfolio Value ($B)", fontsize=11, fontweight="bold")
                ax4.set_title("Largest Portfolios", fontsize=12, fontweight="bold")
                ax4.grid(True, alpha=0.3)

                max_value = values_billions.max()
                ax4.set_ylim(0, max_value * 1.1)

                for i, value in enumerate(values_billions):
                    ax4.text(
                        i,
                        value + max_value * 0.02,
                        f"${value:.0f}B",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                        fontweight="bold",
                    )

            if all(col in df.columns for col in ["first_year", "last_year"]):
                active_managers = df.dropna(subset=["first_year", "last_year"])

                years_range = range(
                    int(active_managers["first_year"].min()), int(active_managers["last_year"].max()) + 1
                )
                active_count = []

                for year in years_range:
                    count = len(
                        active_managers[
                            (active_managers["first_year"] <= year) & (active_managers["last_year"] >= year)
                        ]
                    )
                    active_count.append(count)

                years_list = list(years_range)
                # The final year is still in progress: its filing count is not
                # comparable to the completed years, so it is drawn detached
                # rather than as a genuine decline.
                ax5.plot(years_list[:-1], active_count[:-1], marker="o", linewidth=2, markersize=6)
                if len(years_list) > 1:
                    ax5.plot(
                        years_list[-2:],
                        active_count[-2:],
                        linestyle=":",
                        linewidth=2,
                        color="gray",
                    )
                    ax5.plot(
                        years_list[-1:],
                        active_count[-1:],
                        marker="o",
                        markersize=7,
                        markerfacecolor="white",
                        markeredgecolor="gray",
                        linestyle="none",
                        label=f"{years_list[-1]} (partial year)",
                    )
                    ax5.legend(loc="lower left", fontsize=9)
                ax5.set_xlabel("Year")
                ax5.set_ylabel("Number of Active Managers")
                ax5.set_title("Manager Activity Timeline (final year incomplete)")
                ax5.grid(True, alpha=0.3)

                ax5.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=8))
                ax5.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{int(x)}"))

            analysis_period = self._extract_analysis_period({"manager_track_records": df})
            plt.suptitle(f"Manager Track Record Analysis ({analysis_period})", fontsize=18, fontweight="bold")

            output_path = self.output_dir / "manager_performance_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating manager performance chart: {e}")
            return None

    def create_crisis_alpha_chart(self, df: pd.DataFrame) -> str:
        """Create crisis alpha generators visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()

            n_managers = len(df)
            subset_note = f"top {n_managers} managers by crisis buy-activity score"

            if "crisis_alpha_score" in df.columns:
                top_crisis = df.nlargest(15, "crisis_alpha_score")
                manager_name_col = "manager_name" if "manager_name" in top_crisis.columns else "manager"
                _ = axes[0].barh(
                    top_crisis[manager_name_col], top_crisis["crisis_alpha_score"], color="darkred", alpha=0.7
                )
                axes[0].set_xlabel("Crisis Buy-Activity Score (buy ratio \u00d7 10 \u00d7 crisis periods)", fontweight="bold")
                axes[0].set_title("Top 15 by Crisis Buy-Activity Score", fontsize=12, fontweight="bold")
                axes[0].invert_yaxis()
                axes[0].grid(True, alpha=0.3)

                max_score = top_crisis["crisis_alpha_score"].max()
                axes[0].set_xlim(0, max_score * 1.1)  # Add padding for labels
                for i, (_, row) in enumerate(top_crisis.iterrows()):
                    score = row["crisis_alpha_score"]
                    axes[0].text(
                        score + max_score * 0.02,
                        i,
                        f"{score:.1f}",
                        va="center",
                        ha="left",
                        fontsize=9,
                        fontweight="bold",
                    )

            if all(col in df.columns for col in ["total_crisis_activities", "crisis_alpha_score"]):
                axes[1].scatter(
                    df["total_crisis_activities"],
                    df["crisis_alpha_score"],
                    s=100,
                    alpha=0.6,
                    color="red",
                    edgecolors="black",
                    linewidth=0.5,
                )
                axes[1].set_xlabel("Total Crisis Activities", fontweight="bold")
                axes[1].set_ylabel("Crisis Buy-Activity Score", fontweight="bold")
                axes[1].set_title(f"Crisis Activity vs Buy-Activity Score\n({subset_note})", fontsize=12, fontweight="bold")
                axes[1].grid(True, alpha=0.3)

                top_performers = df.nlargest(5, "crisis_alpha_score")
                for i, (idx, row) in enumerate(top_performers.iterrows()):
                    manager_name_col = "manager_name" if "manager_name" in row else "manager"
                    # Point the label inward: labels on points near the right
                    # edge run off the axes, and near the left edge they
                    # overprinted the y-axis label.
                    x_mid = df["total_crisis_activities"].max() / 2
                    to_left = row["total_crisis_activities"] > x_mid
                    offset = (-6, 6) if to_left else (6, 6)
                    ha = "right" if to_left else "left"

                    axes[1].annotate(
                        self._short_name(row[manager_name_col], 15),
                        (row["total_crisis_activities"], row["crisis_alpha_score"]),
                        xytext=offset,
                        textcoords="offset points",
                        fontsize=8,
                        fontweight="bold",
                        ha=ha,
                        bbox=dict(boxstyle="round,pad=0.2", facecolor="yellow", alpha=0.7),
                    )
                # NOTE: no invert_yaxis() here — this is a scatter, and
                # inverting rendered the best crisis-alpha scores at the
                # BOTTOM of the chart (higher should read as up).

            if all(col in df.columns for col in ["buy_during_crisis", "total_crisis_activities"]):
                # Local series - never mutate the caller's results frame.
                buy_ratio = df["buy_during_crisis"] / df["total_crisis_activities"]
                axes[2].hist(buy_ratio, bins=20, color="green", alpha=0.7)
                axes[2].set_xlabel("Crisis Buy Ratio (buys / crisis actions)")
                axes[2].set_ylabel("Number of Managers")
                axes[2].set_title(f"Buying Behavior During Crises\n({subset_note})", fontsize=12, fontweight="bold")
                axes[2].axvline(
                    x=buy_ratio.mean(),
                    color="red",
                    linestyle="--",
                    label=f"Subset mean: {buy_ratio.mean():.2f}",
                )
                axes[2].legend()

            if "crisis_periods_active" in df.columns:
                period_counts = df["crisis_periods_active"].value_counts().sort_index()
                # Single colour: a red/orange/green list is positional, so it
                # implies a ranking that is really just index order.
                axes[3].bar(period_counts.index, period_counts.values, color="darkred", alpha=0.7)
                axes[3].set_xlabel("Number of Crisis Periods Active")
                axes[3].set_ylabel("Number of Managers")
                axes[3].set_title(f"Crisis Participation Frequency\n({subset_note})", fontsize=12, fontweight="bold")
                axes[3].set_xticks(period_counts.index)
                axes[3].grid(True, alpha=0.3, axis="y")

            plt.suptitle(
                "Crisis-Period Buying Behavior\n"
                f"Score = crisis buy ratio \u00d7 10 \u00d7 crisis periods active - not a return measure ({n_managers} managers)",
                fontsize=16,
                fontweight="bold",
            )
            plt.tight_layout()

            output_path = self.output_dir / "crisis_alpha_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating crisis alpha chart: {e}")
            return None

    def create_position_sizing_chart(self, df: pd.DataFrame) -> str:
        """Create position sizing mastery visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()

            n_managers = len(df)

            if all(col in df.columns for col in ["sizing_efficiency_score", "avg_position_size"]):
                # sizing_efficiency_score is a coarse bucketed score, so points
                # stack on a handful of rows: smaller, more transparent markers
                # keep the overplotted rows readable.
                scatter = axes[0].scatter(
                    df["avg_position_size"],
                    df["sizing_efficiency_score"],
                    s=60,
                    alpha=0.5,
                    c=df["position_concentration"],
                    cmap="plasma",
                    edgecolors="black",
                    linewidth=0.4,
                )
                axes[0].set_xlabel("Average Position Size (%)")
                axes[0].set_ylabel("Sizing Efficiency Score (bucketed)")
                axes[0].set_title(f"Position Sizing Efficiency (top {n_managers} by sizing efficiency score)")
                cbar = axes[0].figure.colorbar(scatter, ax=axes[0])
                cbar.set_label("Position Concentration")

            if "position_concentration" in df.columns:
                top_concentrated = df.nlargest(12, "position_concentration")
                _ = axes[1].bar(
                    range(len(top_concentrated)), top_concentrated["position_concentration"], color="purple", alpha=0.7
                )
                axes[1].set_xticks(range(len(top_concentrated)))
                manager_name_col = "manager_name" if "manager_name" in top_concentrated.columns else "manager"
                axes[1].set_xticklabels(
                    [self._short_name(n, 16) for n in top_concentrated[manager_name_col]], rotation=45, ha="right"
                )
                axes[1].set_ylabel("Position Concentration (%)", fontweight="bold")
                axes[1].set_title(
                    f"Most Concentrated Portfolios (of top {n_managers} by sizing efficiency)", fontsize=12, fontweight="bold"
                )
                axes[1].grid(True, alpha=0.3, axis="y")

            if all(col in df.columns for col in ["small_positions_pct", "medium_positions_pct", "large_positions_pct"]):
                manager_name_col = "manager_name" if "manager_name" in df.columns else "manager"
                top_10_managers = df.nlargest(10, "sizing_efficiency_score")

                x = range(len(top_10_managers))
                bars_small = axes[2].bar(
                    x, top_10_managers["small_positions_pct"], label="Small (<2%)", color="lightcoral"
                )
                bars_medium = axes[2].bar(
                    x,
                    top_10_managers["medium_positions_pct"],
                    bottom=top_10_managers["small_positions_pct"],
                    label="Medium (2-5%)",
                    color="gold",
                )
                bars_large = axes[2].bar(
                    x,
                    top_10_managers["large_positions_pct"],
                    bottom=top_10_managers["small_positions_pct"] + top_10_managers["medium_positions_pct"],
                    label="Large (>5%)",
                    color="green",
                )

                axes[2].set_xticks(x)
                axes[2].set_xticklabels(
                    [self._short_name(n, 16) for n in top_10_managers[manager_name_col]], rotation=45, ha="right"
                )
                axes[2].set_ylabel("Position Percentage (%)", fontweight="bold")
                axes[2].set_title(
                    "Position Size Distribution\n(Top 10, ordered by sizing efficiency score)",
                    fontsize=12,
                    fontweight="bold",
                )
                axes[2].grid(True, alpha=0.3, axis="y")

                # Legend built from the actual bar handles and drawn inside the
                # panel: a positional label list silently mislabels if the bar
                # order ever changes, and bbox_to_anchor put it in the gutter.
                axes[2].set_ylim(0, 120)
                axes[2].legend(
                    handles=[bars_small, bars_medium, bars_large],
                    loc="upper center",
                    ncol=3,
                    fontsize=8,
                    frameon=True,
                    framealpha=0.95,
                )

            if "sizing_style" in df.columns:
                style_counts = df["sizing_style"].value_counts()
                axes[3].pie(style_counts.values, labels=style_counts.index, autopct="%1.1f%%", startangle=90)
                axes[3].set_title(f"Distribution of Sizing Styles\n(top {n_managers} by sizing efficiency score)")

            plt.suptitle(
                f"Position Sizing Analysis (top {n_managers} managers by sizing efficiency score)",
                fontsize=16,
                fontweight="bold",
            )
            plt.tight_layout()

            output_path = self.output_dir / "position_sizing_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating position sizing chart: {e}")
            return None

    def create_evolution_chart(self, df: pd.DataFrame) -> str:
        """Create manager evolution patterns visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()

            n_managers = len(df)

            if "evolution_type" in df.columns:
                evolution_counts = df["evolution_type"].value_counts()
                wedges, wedge_labels, wedge_pcts = axes[0].pie(
                    evolution_counts.values, labels=evolution_counts.index, autopct="%1.1f%%", startangle=90
                )
                self._fold_thin_wedge_pct(wedges, wedge_labels, wedge_pcts)
                axes[0].set_title(f"Manager Evolution Types\n(top {n_managers} by evolution score)")

            if "evolution_score" in df.columns:
                top_evolving = df.nlargest(15, "evolution_score")
                manager_name_col = "manager_name" if "manager_name" in top_evolving.columns else "manager"
                _ = axes[1].barh(
                    top_evolving[manager_name_col], top_evolving["evolution_score"], color="teal", alpha=0.7
                )
                axes[1].set_xlabel("Evolution Score", fontweight="bold")
                axes[1].set_title(f"Top {len(top_evolving)} Evolution Scores", fontsize=12, fontweight="bold")
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3)

                max_score = top_evolving["evolution_score"].max()
                axes[1].set_xlim(0, max_score * 1.1)  # Add padding for labels
                for i, (_, row) in enumerate(top_evolving.iterrows()):
                    score = row["evolution_score"]
                    axes[1].text(
                        score + max_score * 0.02,
                        i,
                        f"{score:.1f}",
                        va="center",
                        ha="left",
                        fontsize=9,
                        fontweight="bold",
                    )

            if all(col in df.columns for col in ["career_length_years", "style_change_score"]):
                axes[2].scatter(df["career_length_years"], df["style_change_score"], s=80, alpha=0.6, color="coral")
                axes[2].set_xlabel("Career Length (Years)")
                axes[2].set_ylabel("Style Change Score")
                axes[2].set_title(f"Experience vs Style Evolution (top {n_managers} by evolution score)")

            # The analyzer emits early_buy_pct / late_buy_pct — the previous
            # gate checked *_ratio names that never exist, so this panel
            # rendered permanently blank.
            if all(col in df.columns for col in ["early_buy_pct", "late_buy_pct"]):
                axes[3].scatter(
                    df["early_buy_pct"],
                    df["late_buy_pct"],
                    s=80,
                    alpha=0.6,
                    color="purple",
                    edgecolors="black",
                    linewidth=0.5,
                )

                axes[3].plot([0, 100], [0, 100], "r--", alpha=0.7, linewidth=2.5, label="No Change", zorder=1)

                axes[3].set_xlabel("Early Career Buy Ratio (%)", fontweight="bold")
                axes[3].set_ylabel("Late Career Buy Ratio (%)", fontweight="bold")
                axes[3].set_title(f"Buy Behavior Evolution (top {n_managers} by evolution score)", fontsize=12, fontweight="bold")
                axes[3].grid(True, alpha=0.3)
                axes[3].legend(loc="upper left", fontsize=10, frameon=True, fancybox=True)

                # Local series - never mutate the caller's results frame.
                buy_change = (df["late_buy_pct"] - df["early_buy_pct"]).abs()
                top_changers = df.loc[buy_change.nlargest(3).index]
                for _, row in top_changers.iterrows():
                    manager_name_col = "manager_name" if "manager_name" in row else "manager"
                    # Offset the text and flip it inward near the right edge so
                    # points sitting on the axis maximum are not clipped.
                    to_left = row["early_buy_pct"] > 75
                    axes[3].annotate(
                        self._short_name(row[manager_name_col], 20),
                        (row["early_buy_pct"], row["late_buy_pct"]),
                        xytext=(-6, 6) if to_left else (6, 6),
                        textcoords="offset points",
                        ha="right" if to_left else "left",
                        fontsize=8,
                    )

            plt.suptitle(f"Manager Evolution Patterns (top {n_managers} managers by evolution score)", fontsize=16, fontweight="bold")
            plt.tight_layout()

            output_path = self.output_dir / "manager_evolution_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating evolution chart: {e}")
            return None

    def create_consensus_picks_chart(self, df: pd.DataFrame) -> str:
        """Create multi-manager consensus picks visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()

            # Rank the top-20 by the metric the bars actually display
            # (manager_count). Taking df.head(20) inherited the analyzer's
            # consensus_score ordering, producing visibly non-monotonic bars
            # under an axis labeled "Number of Managers".
            top_consensus = (
                df.nlargest(20, "manager_count") if "manager_count" in df.columns else df.head(20)
            )

            _ = axes[0].barh(top_consensus["ticker"], top_consensus["manager_count"], color="darkblue", alpha=0.7)
            axes[0].set_xlabel("Number of Managers", fontweight="bold")
            # This frame is the analyzer's 50 highest consensus scores, not the
            # whole universe, so tickers held by more managers but scoring lower
            # (BAC, JPM, CMCSA, ORCL ...) are absent. The title has to say so.
            axes[0].set_title(
                f"Top {len(top_consensus)} by Manager Count\n"
                f"(among the {len(df)} highest consensus scores, not all stocks)",
                fontsize=12,
                fontweight="bold",
            )
            axes[0].invert_yaxis()
            axes[0].grid(True, alpha=0.3)

            max_count = top_consensus["manager_count"].max()
            axes[0].set_xlim(0, max_count * 1.1)  # Add padding for labels
            for i, (ticker, count) in enumerate(zip(top_consensus["ticker"], top_consensus["manager_count"])):
                axes[0].text(
                    count + max_count * 0.02, i, f"{count}", va="center", ha="left", fontsize=9, fontweight="bold"
                )

            if "avg_portfolio_pct" in df.columns:
                axes[1].scatter(
                    top_consensus["manager_count"], top_consensus["avg_portfolio_pct"], s=100, alpha=0.6, color="green"
                )
                axes[1].set_xlabel("Number of Managers")
                axes[1].set_ylabel("Average Portfolio %")
                axes[1].set_title(f"Consensus vs Concentration (the {len(top_consensus)} picks shown above)")
                axes[1].margins(x=0.12, y=0.1)
                self._annotate_without_overlap(
                    axes[1],
                    top_consensus["manager_count"],
                    top_consensus["avg_portfolio_pct"],
                    top_consensus["ticker"],
                    fontsize=8,
                    alpha=0.7,
                )

            axes[2].hist(df["manager_count"], bins=20, color="purple", alpha=0.7, edgecolor="black", linewidth=0.5)
            axes[2].set_xlabel("Number of Managers Holding", fontweight="bold")
            axes[2].set_ylabel("Number of Stocks", fontweight="bold")
            axes[2].set_title(
                f"Manager Consensus Distribution\n(top {len(df)} stocks by consensus score, not all stocks)",
                fontsize=12,
                fontweight="bold",
            )
            axes[2].grid(True, alpha=0.3)

            mean_value = df["manager_count"].mean()
            axes[2].axvline(
                x=mean_value,
                color="red",
                linestyle="--",
                linewidth=2.5,
                label=f"Mean of these {len(df)}: {mean_value:.1f}",
                alpha=0.8,
            )
            axes[2].legend(loc="upper right", fontsize=10, frameon=True, fancybox=True)

            # The analyzer emits "top_managers" (a truncated preview list);
            # the previous gate checked a "managers" column that never
            # exists, so this panel rendered permanently blank.
            managers_col = next((c for c in ["managers", "top_managers"] if c in df.columns), None)
            if managers_col:
                # The CSV writer truncates these lists to 5 names + "+N more".
                # Counting a truncated preview would silently under-report every
                # manager, so refuse to plot rather than render a wrong ranking.
                truncated = sum(
                    1
                    for v in top_consensus[managers_col]
                    if pd.notna(v) and any(part.strip().startswith("+") for part in str(v).split(","))
                )
                if truncated:
                    axes[3].text(
                        0.5,
                        0.5,
                        f"Manager lists are truncated previews for\n{truncated} of {len(top_consensus)} tickers"
                        "\n(counts would be understated - panel omitted)",
                        ha="center",
                        va="center",
                        fontsize=11,
                        transform=axes[3].transAxes,
                    )
                    axes[3].set_axis_off()
                else:
                    manager_appearances = {}
                    for managers_str in top_consensus[managers_col]:
                        if pd.notna(managers_str):
                            for mgr in str(managers_str).split(","):
                                mgr = mgr.strip()
                                if mgr:
                                    manager_appearances[mgr] = manager_appearances.get(mgr, 0) + 1

                    top_consensus_mgrs = sorted(manager_appearances.items(), key=lambda x: x[1], reverse=True)[:10]
                    axes[3].bar(
                        [self._short_name(m[0], 20) for m in top_consensus_mgrs],
                        [m[1] for m in top_consensus_mgrs],
                        color="darkgreen",
                    )
                    axes[3].set_xlabel("Manager")
                    axes[3].set_ylabel(f"Appearances Among the {len(top_consensus)} Picks Shown")
                    axes[3].set_title(
                        f"Managers Most Aligned with the {len(top_consensus)} Picks Shown\n"
                        f"(counted over panel 1 only, not all stocks)"
                    )
                    axes[3].tick_params(axis="x", rotation=45)
                    for label in axes[3].get_xticklabels():
                        label.set_ha("right")

            plt.suptitle(f"Multi-Manager Consensus Analysis (top {len(df)} stocks by consensus score)", fontsize=16, fontweight="bold")
            plt.tight_layout()

            output_path = self.output_dir / "consensus_picks_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating consensus picks chart: {e}")
            return None

    def create_top_holdings_chart(self, df: pd.DataFrame) -> str:
        """Create top holdings analysis visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()

            top_by_value = df.nlargest(15, "total_value")

            values_billions = top_by_value["total_value"] / 1e9
            _ = axes[0].barh(top_by_value["ticker"], values_billions, color="gold", alpha=0.7)
            axes[0].set_xlabel("Total Value ($B)", fontweight="bold")
            # The frame is the analyzer's 50 most widely held tickers, so
            # universe-wide value leaders that few managers hold (KO, OXY ...)
            # are not in it. Rank honestly inside the frame we were given.
            axes[0].set_title(
                f"Top {len(top_by_value)} by Total Value\n(among the {len(df)} most widely held)",
                fontsize=12,
                fontweight="bold",
            )
            axes[0].invert_yaxis()
            axes[0].grid(True, alpha=0.3)

            max_value = values_billions.max()
            axes[0].set_xlim(0, max_value * 1.1)  # Add padding for labels
            for i, (ticker, value) in enumerate(zip(top_by_value["ticker"], values_billions)):
                axes[0].text(
                    value + max_value * 0.02, i, f"${value:.1f}B", va="center", ha="left", fontsize=9, fontweight="bold"
                )

            if "manager_count" in df.columns:
                axes[1].scatter(
                    df["total_value"] / 1e9,
                    df["manager_count"],
                    s=60,
                    alpha=0.5,
                    color="navy",
                    edgecolors="black",
                    linewidth=0.5,
                )
                axes[1].set_xlabel("Total Value ($B) - Log Scale", fontweight="bold")
                axes[1].set_ylabel("Number of Managers", fontweight="bold")
                axes[1].set_title(f"Value vs Manager Interest (all {len(df)} top holdings)", fontsize=12, fontweight="bold")
                axes[1].set_xscale("log")
                axes[1].grid(True, alpha=0.3)

                from matplotlib.ticker import FuncFormatter

                axes[1].xaxis.set_major_formatter(FuncFormatter(lambda x, p: f"${int(x)}B" if x >= 1 else f"${x:.1f}B"))

            if "max_portfolio_pct" in df.columns and "avg_portfolio_pct" in df.columns:
                axes[2].scatter(df["avg_portfolio_pct"], df["max_portfolio_pct"], s=60, alpha=0.6, color="red")
                axes[2].set_xlabel("Average Portfolio Percentage (%)")
                axes[2].set_ylabel("Maximum Portfolio Percentage (%)")
                axes[2].set_title(f"Position Concentration Patterns (all {len(df)} top holdings)")

                max_val = max(df["max_portfolio_pct"].max(), df["avg_portfolio_pct"].max())
                axes[2].plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="Equal avg/max")
                axes[2].legend()

            if "avg_portfolio_pct" in df.columns:
                # Rank by the metric this panel plots; reusing the by-value
                # ordering produced visibly non-monotonic bars.
                top_by_weight = df.nlargest(15, "avg_portfolio_pct")
                axes[3].bar(range(len(top_by_weight)), top_by_weight["avg_portfolio_pct"], color="darkgreen")
                axes[3].set_xticks(range(len(top_by_weight)))
                axes[3].set_xticklabels(top_by_weight["ticker"], rotation=45, ha="right")
                axes[3].set_ylabel("Average Portfolio Allocation (%)")
                axes[3].set_title(
                    f"Top {len(top_by_weight)} by Average Portfolio Weight\n"
                    f"(among the {len(df)} most widely held)"
                )

            plt.suptitle(f"Top Holdings Analysis (top {len(df)} tickers by manager count and value)", fontsize=16, fontweight="bold")
            plt.tight_layout()

            output_path = self.output_dir / "top_holdings_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating top holdings chart: {e}")
            return None

    def create_accumulation_distribution_chart(self, df: pd.DataFrame) -> str:
        """Create accumulation vs distribution phase visualization for STOCKS (aggregated across managers)."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(18, 14))
            axes = axes.flatten()

            # Chart 1: Phase Distribution Pie Chart
            phase_counts = df["phase"].value_counts()
            colors = {"Accumulating": "#2ecc71", "Distributing": "#e74c3c", "Mixed": "#f39c12"}
            pie_colors = [colors.get(phase, "#95a5a6") for phase in phase_counts.index]

            axes[0].pie(
                phase_counts.values,
                labels=phase_counts.index,
                autopct="%1.1f%%",
                colors=pie_colors,
                startangle=90,
                textprops={"fontsize": 11, "fontweight": "bold"},
            )
            # This frame is the analyzer's top-N slice ranked by |net_activity|
            # over stocks with >=2 active managers, so "Mixed" (net == 0) sorts
            # last and can never appear. Say so instead of implying a
            # market-wide accumulate/distribute split.
            axes[0].set_title(
                f"Activity Phase Mix of the {len(df)} Most-Traded Stocks\n"
                "(ranked by |net activity|, last 4 quarters, \u22652 managers;\n"
                "net-zero 'Mixed' stocks cannot enter this ranking)",
                fontsize=11,
                fontweight="bold",
            )

            # Chart 2: Top 20 Stocks Being Accumulated
            accumulating = df[df["phase"] == "Accumulating"].nlargest(20, "net_activity")
            if not accumulating.empty:
                # Create labels with company names (or ticker if no name)
                labels = []
                for _, row in accumulating.iterrows():
                    company = row.get("company_name", "")
                    ticker = row["ticker"]
                    if company and str(company) != "nan":
                        label = f"{ticker} ({self._short_name(company, 22)})"
                    else:
                        label = ticker
                    labels.append(label)

                _ = axes[1].barh(
                    range(len(accumulating)),
                    accumulating["net_activity"],
                    color="#2ecc71",
                    alpha=0.7,
                    edgecolor="darkgreen",
                    linewidth=1,
                )
                axes[1].set_yticks(range(len(accumulating)))
                axes[1].set_yticklabels(labels, fontsize=8)
                axes[1].set_xlabel("Net Buying Actions (Last 4 Quarters)", fontweight="bold", fontsize=11)
                axes[1].set_title(
                    f"Top {len(accumulating)} Stocks Being Accumulated\n(of the {len(df)} most-traded, last 4 quarters)",
                    fontsize=12,
                    fontweight="bold",
                )
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3, axis="x")
                # Headroom so the value labels do not run into the panel edge.
                axes[1].set_xlim(0, accumulating["net_activity"].max() * 1.25)

                # Add value labels with manager count
                for i, (_, row) in enumerate(accumulating.iterrows()):
                    val = row["net_activity"]
                    mgrs = row.get("unique_managers", 0)
                    axes[1].text(
                        val + 0.5, i, f"+{val} ({mgrs}M)", va="center", ha="left", fontsize=8, fontweight="bold"
                    )

            else:
                axes[1].text(
                    0.5,
                    0.5,
                    "No accumulating stocks found",
                    ha="center",
                    va="center",
                    fontsize=12,
                    transform=axes[1].transAxes,
                )

            # Chart 3: Top 20 Stocks Being Distributed
            distributing = df[df["phase"] == "Distributing"].nsmallest(20, "net_activity")
            if not distributing.empty:
                # Create labels with company names
                labels = []
                for _, row in distributing.iterrows():
                    company = row.get("company_name", "")
                    ticker = row["ticker"]
                    if company and str(company) != "nan":
                        label = f"{ticker} ({self._short_name(company, 22)})"
                    else:
                        label = ticker
                    labels.append(label)

                _ = axes[2].barh(
                    range(len(distributing)),
                    distributing["net_activity"],
                    color="#e74c3c",
                    alpha=0.7,
                    edgecolor="darkred",
                    linewidth=1,
                )
                axes[2].set_yticks(range(len(distributing)))
                axes[2].set_yticklabels(labels, fontsize=8)
                axes[2].set_xlabel("Net Selling Actions (Last 4 Quarters)", fontweight="bold", fontsize=11)
                axes[2].set_title(
                    f"Top {len(distributing)} Stocks Being Distributed\n(of the {len(df)} most-traded, last 4 quarters)",
                    fontsize=12,
                    fontweight="bold",
                )
                axes[2].invert_yaxis()
                axes[2].grid(True, alpha=0.3, axis="x")
                axes[2].set_xlim(distributing["net_activity"].min() * 1.1, 0)

                # Add value labels with manager count
                for i, (_, row) in enumerate(distributing.iterrows()):
                    val = row["net_activity"]
                    mgrs = row.get("unique_managers", 0)
                    # Anchor value labels at the zero end: at the bar tip they
                    # overprinted the y tick labels on the same side.
                    axes[2].text(
                        0.5, i, f"{val} ({mgrs}M)", va="center", ha="left", fontsize=8, fontweight="bold"
                    )
            else:
                axes[2].text(
                    0.5,
                    0.5,
                    "No distributing stocks found",
                    ha="center",
                    va="center",
                    fontsize=12,
                    transform=axes[2].transAxes,
                )

            # Chart 4: Buy/Sell Activity Scatter - show stocks with most activity
            top_activity = df.nlargest(50, "unique_managers")

            scatter_colors = [colors.get(p, "#95a5a6") for p in top_activity["phase"]]
            axes[3].scatter(
                top_activity["buy_add_actions"],
                top_activity["sell_reduce_actions"],
                s=top_activity.get("unique_managers", 1) * 6,  # Size by manager count
                alpha=0.6,
                c=scatter_colors,
                edgecolors="black",
                linewidth=0.5,
            )

            # Add diagonal line (equal buy/sell)
            max_val = max(top_activity["buy_add_actions"].max(), top_activity["sell_reduce_actions"].max())
            axes[3].plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="Equal Buy/Sell", linewidth=2)

            axes[3].set_xlabel("Total Buy + Add Actions", fontweight="bold", fontsize=11)
            axes[3].set_ylabel("Total Sell + Reduce Actions", fontweight="bold", fontsize=11)
            axes[3].set_title(
                "Stock Activity Pattern\n(Top 50 by Manager Count - Size = # Managers)", fontsize=12, fontweight="bold"
            )

            # Add legend for colors
            from matplotlib.patches import Patch

            # Only list phases that are actually present in this panel.
            present_phases = [p for p in ["Accumulating", "Distributing", "Mixed"] if p in set(top_activity["phase"])]
            legend_elements = [Patch(facecolor=colors[p], label=p) for p in present_phases]
            acc_legend = axes[3].legend(handles=legend_elements, loc="upper left")
            axes[3].grid(True, alpha=0.3)

            # Add annotations for most active stocks
            labelled = top_activity.nlargest(10, "unique_managers")
            axes[3].margins(0.08)
            axes[3].figure.canvas.draw()
            legend_bbox = acc_legend.get_window_extent(renderer=axes[3].figure.canvas.get_renderer())
            self._annotate_without_overlap(
                axes[3],
                labelled["buy_add_actions"],
                labelled["sell_reduce_actions"],
                labelled["ticker"],
                fontsize=8,
                avoid=[legend_bbox],
                alpha=0.8,
                fontweight="bold",
            )

            plt.suptitle(
                "Accumulation vs Distribution Analysis\n"
                f"The {len(df)} stocks with the largest net 4-quarter activity (\u22652 managers) - not the full universe",
                fontsize=16,
                fontweight="bold",
            )
            plt.tight_layout()

            output_path = self.output_dir / "accumulation_distribution_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()

            return str(output_path)

        except Exception as e:
            logger.error(f"Error creating accumulation/distribution chart: {e}")
            return None
