#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - File Contracts Module

Defines explicit contracts for every CSV output file, including column specifications,
validation rules, and metadata requirements. Used for ensuring data consistency
and enabling proper validation of generated analysis files.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple, Any, Union
from enum import Enum
import pandas as pd


class ViolationType(Enum):
    """Types of contract violations."""
    
    MISSING_COLUMN = "missing_column"
    UNEXPECTED_COLUMN = "unexpected_column"
    INVALID_DTYPE = "invalid_dtype"
    OUT_OF_RANGE = "out_of_range"
    MISSING_METADATA = "missing_metadata"
    FORBIDDEN_STATE = "forbidden_state"
    PREVIEW_LIMIT_EXCEEDED = "preview_limit_exceeded"
    NULL_IN_REQUIRED = "null_in_required"
    INVALID_HORIZON = "invalid_horizon"


@dataclass
class Violation:
    """Represents a single contract violation."""
    
    violation_type: ViolationType
    column: Optional[str]
    message: str
    severity: Literal["error", "warning"] = "error"
    row_indices: Optional[List[int]] = None
    
    def __str__(self) -> str:
        """String representation of the violation."""
        col_info = f" in column '{self.column}'" if self.column else ""
        return f"[{self.severity.upper()}] {self.violation_type.value}{col_info}: {self.message}"


@dataclass
class ColumnSpec:
    """Specification for a single CSV column.
    
    Attributes:
        name: Column name as it appears in the CSV header.
        meaning: Human-readable description of what this column represents.
        dtype: Expected data type ('str', 'int', 'float', 'bool', 'datetime').
        allowed_range: Optional tuple of (min, max) for numeric columns.
        reconciliation_rule: Optional rule for handling discrepancies.
        is_preview: Whether this column contains preview/truncated data.
        preview_limit: Maximum number of items if is_preview is True.
        nullable: Whether NULL/NaN values are allowed.
        allowed_values: Optional list of allowed discrete values.
    """
    
    name: str
    meaning: str
    dtype: str
    allowed_range: Optional[Tuple[Any, Any]] = None
    reconciliation_rule: Optional[str] = None
    is_preview: bool = False
    preview_limit: Optional[int] = None
    nullable: bool = True
    allowed_values: Optional[List[Any]] = None
    
    def validate_value(self, value: Any) -> Optional[str]:
        """Validate a single value against this column spec.
        
        Returns:
            Error message if validation fails, None otherwise.
        """
        if pd.isna(value):
            if not self.nullable:
                return f"NULL value not allowed in non-nullable column"
            return None
        
        # Check allowed values
        if self.allowed_values is not None and value not in self.allowed_values:
            return f"Value '{value}' not in allowed values: {self.allowed_values}"
        
        # Check range for numeric types
        if self.allowed_range is not None and self.dtype in ('int', 'float'):
            try:
                num_val = float(value)
                min_val, max_val = self.allowed_range
                if min_val is not None and num_val < min_val:
                    return f"Value {num_val} below minimum {min_val}"
                if max_val is not None and num_val > max_val:
                    return f"Value {num_val} above maximum {max_val}"
            except (ValueError, TypeError):
                pass  # Type validation handled elsewhere
        
        return None


@dataclass
class FileContract:
    """Contract defining the structure and constraints for a CSV output file.
    
    Attributes:
        file_name: Name of the CSV file (e.g., 'top_holdings.csv').
        mode: Analysis mode ('current', 'historical', 'advanced').
        description: Human-readable description of the file's purpose.
        allowed_horizons: Time horizon constraint ('recent-only', 'multi-year', 'mixed-explicit').
        required_metadata: List of required metadata column names (prefixed with _).
        forbidden_states: List of states/conditions that should never appear.
        column_specs: Dictionary mapping column names to their specifications.
        min_rows: Minimum expected number of data rows (optional).
        max_rows: Maximum expected number of data rows (optional).
    """
    
    file_name: str
    mode: Literal["current", "historical", "advanced"]
    description: str
    allowed_horizons: str
    required_metadata: List[str]
    forbidden_states: List[str]
    column_specs: Dict[str, ColumnSpec]
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    
    def get_required_columns(self) -> List[str]:
        """Get list of required (non-nullable) column names."""
        return [name for name, spec in self.column_specs.items() if not spec.nullable]
    
    def get_preview_columns(self) -> List[str]:
        """Get list of columns that contain preview/truncated data."""
        return [name for name, spec in self.column_specs.items() if spec.is_preview]


def validate_against_contract(
    df: pd.DataFrame,
    contract: FileContract,
    strict: bool = False
) -> List[Violation]:
    """Validate a DataFrame against a file contract.
    
    Args:
        df: The DataFrame to validate.
        contract: The FileContract to validate against.
        strict: If True, unexpected columns are errors; otherwise warnings.
    
    Returns:
        List of Violation objects describing any contract violations.
    """
    violations: List[Violation] = []
    
    # Check for missing required columns
    expected_columns = set(contract.column_specs.keys())
    actual_columns = set(df.columns)
    
    missing_columns = expected_columns - actual_columns
    for col in missing_columns:
        spec = contract.column_specs[col]
        if not spec.nullable:
            violations.append(Violation(
                violation_type=ViolationType.MISSING_COLUMN,
                column=col,
                message=f"Required column '{col}' is missing",
                severity="error"
            ))
        else:
            violations.append(Violation(
                violation_type=ViolationType.MISSING_COLUMN,
                column=col,
                message=f"Expected column '{col}' is missing",
                severity="warning"
            ))
    
    # Check for unexpected columns (excluding metadata columns starting with _)
    unexpected_columns = actual_columns - expected_columns
    # Filter out standard metadata columns
    standard_metadata = {'_analysis_type', '_generated', '_periods', '_window_quarters'}
    unexpected_non_metadata = unexpected_columns - standard_metadata
    
    for col in unexpected_non_metadata:
        if not col.startswith('_'):
            violations.append(Violation(
                violation_type=ViolationType.UNEXPECTED_COLUMN,
                column=col,
                message=f"Unexpected column '{col}' not in contract",
                severity="error" if strict else "warning"
            ))
    
    # Check required metadata columns
    for meta_col in contract.required_metadata:
        if meta_col not in actual_columns:
            violations.append(Violation(
                violation_type=ViolationType.MISSING_METADATA,
                column=meta_col,
                message=f"Required metadata column '{meta_col}' is missing",
                severity="error"
            ))
    
    # Validate each column's values
    for col_name, spec in contract.column_specs.items():
        if col_name not in df.columns:
            continue
        
        col_data = df[col_name]
        
        # Check for nulls in non-nullable columns
        if not spec.nullable:
            null_mask = col_data.isna()
            if null_mask.any():
                null_indices = df.index[null_mask].tolist()
                violations.append(Violation(
                    violation_type=ViolationType.NULL_IN_REQUIRED,
                    column=col_name,
                    message=f"Found {null_mask.sum()} NULL values in non-nullable column",
                    severity="error",
                    row_indices=null_indices[:10]  # Limit to first 10
                ))
        
        # Check data types
        if spec.dtype == 'int':
            if not pd.api.types.is_integer_dtype(col_data) and not pd.api.types.is_float_dtype(col_data):
                violations.append(Violation(
                    violation_type=ViolationType.INVALID_DTYPE,
                    column=col_name,
                    message=f"Expected integer type, got {col_data.dtype}",
                    severity="warning"
                ))
        elif spec.dtype == 'float':
            if not pd.api.types.is_numeric_dtype(col_data):
                violations.append(Violation(
                    violation_type=ViolationType.INVALID_DTYPE,
                    column=col_name,
                    message=f"Expected numeric type, got {col_data.dtype}",
                    severity="warning"
                ))
        
        # Check value ranges
        if spec.allowed_range is not None and pd.api.types.is_numeric_dtype(col_data):
            min_val, max_val = spec.allowed_range
            if min_val is not None:
                below_min = col_data < min_val
                if below_min.any():
                    violations.append(Violation(
                        violation_type=ViolationType.OUT_OF_RANGE,
                        column=col_name,
                        message=f"Found {below_min.sum()} values below minimum {min_val}",
                        severity="error",
                        row_indices=df.index[below_min].tolist()[:10]
                    ))
            if max_val is not None:
                above_max = col_data > max_val
                if above_max.any():
                    violations.append(Violation(
                        violation_type=ViolationType.OUT_OF_RANGE,
                        column=col_name,
                        message=f"Found {above_max.sum()} values above maximum {max_val}",
                        severity="error",
                        row_indices=df.index[above_max].tolist()[:10]
                    ))
        
        # Check allowed values
        if spec.allowed_values is not None:
            non_null = col_data.dropna()
            invalid_mask = ~non_null.isin(spec.allowed_values)
            if invalid_mask.any():
                invalid_values = non_null[invalid_mask].unique()[:5]
                violations.append(Violation(
                    violation_type=ViolationType.OUT_OF_RANGE,
                    column=col_name,
                    message=f"Found invalid values: {list(invalid_values)}. Allowed: {spec.allowed_values}",
                    severity="error"
                ))
        
        # Check preview limits
        if spec.is_preview and spec.preview_limit is not None:
            for idx, val in col_data.items():
                if pd.notna(val) and isinstance(val, str):
                    # Count items (assuming comma-separated or similar)
                    items = [x.strip() for x in str(val).split(',') if x.strip()]
                    if len(items) > spec.preview_limit:
                        violations.append(Violation(
                            violation_type=ViolationType.PREVIEW_LIMIT_EXCEEDED,
                            column=col_name,
                            message=f"Row {idx}: {len(items)} items exceeds preview limit of {spec.preview_limit}",
                            severity="warning"
                        ))
                        break  # Only report once per column
    
    # Check forbidden states
    for forbidden in contract.forbidden_states:
        # Check if forbidden state appears in any string column
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].astype(str).str.contains(forbidden, case=False, na=False).any():
                violations.append(Violation(
                    violation_type=ViolationType.FORBIDDEN_STATE,
                    column=col,
                    message=f"Forbidden state '{forbidden}' found in column",
                    severity="error"
                ))
    
    # Check row count constraints
    row_count = len(df)
    if contract.min_rows is not None and row_count < contract.min_rows:
        violations.append(Violation(
            violation_type=ViolationType.OUT_OF_RANGE,
            column=None,
            message=f"Row count {row_count} below minimum {contract.min_rows}",
            severity="warning"
        ))
    if contract.max_rows is not None and row_count > contract.max_rows:
        violations.append(Violation(
            violation_type=ViolationType.OUT_OF_RANGE,
            column=None,
            message=f"Row count {row_count} above maximum {contract.max_rows}",
            severity="warning"
        ))
    
    return violations


# =============================================================================
# STANDARD METADATA COLUMNS (present in most files)
# =============================================================================

STANDARD_METADATA_COLUMNS = {
    "_analysis_type": ColumnSpec(
        name="_analysis_type",
        meaning="Type of analysis that generated this file",
        dtype="str",
        nullable=False
    ),
    "_generated": ColumnSpec(
        name="_generated",
        meaning="Date when this analysis was generated (YYYY-MM-DD)",
        dtype="str",
        nullable=False
    ),
    "_periods": ColumnSpec(
        name="_periods",
        meaning="Quarters included in this analysis (comma-separated)",
        dtype="str",
        nullable=True
    ),
    "_window_quarters": ColumnSpec(
        name="_window_quarters",
        meaning="Number of quarters in the analysis window",
        dtype="int",
        nullable=True
    ),
}


# =============================================================================
# CURRENT MODE FILE CONTRACTS (18 files)
# =============================================================================

CONTRACT_52_WEEK_HIGH_SELLS = FileContract(
    file_name="52_week_high_sells.csv",
    mode="current",
    description="Stocks being sold near their 52-week highs, indicating potential profit-taking by managers",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["N/A", "ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value of holdings across managers", "float", (0, None)),
        "portfolio_pct": ColumnSpec("portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "sell_count": ColumnSpec("sell_count", "Number of sell/reduce actions", "int", (0, None)),
        "selling_managers": ColumnSpec(
            "selling_managers", "Names of managers selling", "str",
            is_preview=True, preview_limit=10
        ),
        "shares_sold": ColumnSpec("shares_sold", "Total shares sold", "int", (0, None)),
        "periods": ColumnSpec("periods", "Quarters with activity", "str"),
        "activities": ColumnSpec("activities", "Activity details (truncated)", "str", is_preview=True),
        "52_week_low": ColumnSpec("52_week_low", "52-week low price", "float", (0, None)),
        "52_week_high": ColumnSpec("52_week_high", "52-week high price", "float", (0, None)),
        "52_week_position_pct": ColumnSpec("52_week_position_pct", "Position in 52-week range (0-100%)", "float", (0, 100)),
        "near_52w_high": ColumnSpec("near_52w_high", "Whether stock is near 52-week high", "bool"),
        "profit_taking_score": ColumnSpec("profit_taking_score", "Score indicating profit-taking intensity", "float", (0, None)),
        "selling_type": ColumnSpec(
            "selling_type", "Classification of selling behavior", "str",
            allowed_values=["Heavy Distribution", "Moderate Distribution", "Light Distribution", "Profit Taking", "Peak Selling"]
        ),
    }
)

CONTRACT_52_WEEK_LOW_BUYS = FileContract(
    file_name="52_week_low_buys.csv",
    mode="current",
    description="Stocks being bought near their 52-week lows, indicating value accumulation",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["N/A", "ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value of holdings across managers", "float", (0, None)),
        "portfolio_pct": ColumnSpec("portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "buy_count": ColumnSpec("buy_count", "Number of buy/add actions", "int", (0, None)),
        "buying_managers": ColumnSpec(
            "buying_managers", "Names of managers buying", "str",
            is_preview=True, preview_limit=10
        ),
        "shares_bought": ColumnSpec("shares_bought", "Total shares bought", "int", (0, None)),
        "periods": ColumnSpec("periods", "Quarters with activity", "str"),
        "activities": ColumnSpec("activities", "Activity details (truncated)", "str", is_preview=True),
        "52_week_low": ColumnSpec("52_week_low", "52-week low price", "float", (0, None)),
        "52_week_high": ColumnSpec("52_week_high", "52-week high price", "float", (0, None)),
        "52_week_position_pct": ColumnSpec("52_week_position_pct", "Position in 52-week range (0-100%)", "float", (0, 100)),
        "near_52w_low": ColumnSpec("near_52w_low", "Whether stock is near 52-week low", "bool"),
        "value_opportunity_score": ColumnSpec("value_opportunity_score", "Score indicating value opportunity", "float", (0, None)),
        "opportunity_type": ColumnSpec(
            "opportunity_type", "Classification of opportunity", "str",
            allowed_values=["Strong Accumulation", "Deep Value", "Value Accumulation", "Moderate Accumulation", "Value Buying"]
        ),
    }
)

CONTRACT_CONCENTRATION_CHANGES = FileContract(
    file_name="concentration_changes.csv",
    mode="current",
    description="Significant changes in portfolio concentration for individual manager-stock pairs",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_value": ColumnSpec("total_value", "Current total value of position", "float", (0, None)),
        "portfolio_pct": ColumnSpec("portfolio_pct", "Current portfolio percentage", "float", (0, 100)),
        "recent_actions": ColumnSpec("recent_actions", "Summary of recent actions", "str"),
        "action_details": ColumnSpec("action_details", "Detailed action descriptions", "str"),
        "periods": ColumnSpec("periods", "Quarters with activity", "str"),
        "change_type": ColumnSpec(
            "change_type", "Type of concentration change", "str",
            allowed_values=["Increased", "Reduced", "New Position", "Exited", "Stable", "New High Conviction", "Partial Exit", "Maintained"]
        ),
        "concentration_score": ColumnSpec("concentration_score", "Score measuring concentration level", "float", (0, 100)),
    }
)

CONTRACT_CONTRARIAN_OPPORTUNITIES = FileContract(
    file_name="contrarian_opportunities.csv",
    mode="current",
    description="Stocks with significant divergence between buying and selling activity",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "total_value": ColumnSpec("total_value", "Total value across all holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "active_managers": ColumnSpec("active_managers", "Number of active managers", "float", (0, None)),
        "periods": ColumnSpec("periods", "Quarters included", "str"),
        "buy_count": ColumnSpec("buy_count", "Number of buy actions", "float", (0, None)),
        "sell_count": ColumnSpec("sell_count", "Number of sell actions", "float", (0, None)),
        "current_holders": ColumnSpec("current_holders", "Number of current holders", "float", (0, None)),
        "contrarian_score": ColumnSpec("contrarian_score", "Contrarian opportunity score", "float"),
        "contrarian_signal": ColumnSpec(
            "contrarian_signal", "Direction of contrarian signal", "str",
            allowed_values=["Net Buying", "Net Selling", "Mixed", "Neutral", "Mixed Signal"]
        ),
        "buying_managers": ColumnSpec("buying_managers", "Names of buying managers", "str", is_preview=True),
        "selling_managers": ColumnSpec("selling_managers", "Names of selling managers", "str", is_preview=True),
        "add_count": ColumnSpec("add_count", "Number of add actions", "float"),
        "reduce_count": ColumnSpec("reduce_count", "Number of reduce actions", "float"),
    }
)

CONTRACT_DEEP_VALUE_PLAYS = FileContract(
    file_name="deep_value_plays.csv",
    mode="current",
    description="Stocks trading at significant discounts to reported prices, indicating deep value opportunities",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
        "managers": ColumnSpec("managers", "Names of managers", "str", is_preview=True),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "index": ColumnSpec("index", "Value index score", "int", (0, None)),
        "avg_reported_price": ColumnSpec("avg_reported_price", "Average price at which managers reported", "float", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "price_change_pct": ColumnSpec("price_change_pct", "Percentage change from reported price", "float"),
        "52_week_low": ColumnSpec("52_week_low", "52-week low price", "float", (0, None)),
        "52_week_high": ColumnSpec("52_week_high", "52-week high price", "float", (0, None)),
        "52_week_position_pct": ColumnSpec("52_week_position_pct", "Position in 52-week range", "float", (0, 100)),
        "near_52w_low": ColumnSpec("near_52w_low", "Whether near 52-week low", "bool"),
        "value_score": ColumnSpec("value_score", "Overall value score", "float", (0, None)),
        "recent_buys": ColumnSpec("recent_buys", "Number of recent buy actions", "float", (0, None)),
        "value_type": ColumnSpec(
            "value_type", "Classification of value opportunity", "str",
            allowed_values=["Deep Discount", "Moderate Discount", "Value Play", "Fair Value", "52-Week Low Value"]
        ),
    }
)

CONTRACT_HIGH_CONVICTION_LOW_PRICE = FileContract(
    file_name="high_conviction_low_price.csv",
    mode="current",
    description="High conviction holdings with low absolute prices, potential bargains",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
        "managers": ColumnSpec("managers", "Names of managers", "str", is_preview=True),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "index": ColumnSpec("index", "Conviction index", "int", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
        "conviction_price_score": ColumnSpec("conviction_price_score", "Combined conviction and price score", "float", (0, None)),
        "recent_buys": ColumnSpec("recent_buys", "Number of recent buy actions", "float", (0, None)),
        "opportunity_type": ColumnSpec(
            "opportunity_type", "Classification of opportunity", "str",
            allowed_values=["Deep Conviction Bargain", "High Conviction Value", "Moderate Conviction", "Speculative", "High Conviction Low Price"]
        ),
    }
)

CONTRACT_HIGHEST_PORTFOLIO_CONCENTRATION = FileContract(
    file_name="highest_portfolio_concentration.csv",
    mode="current",
    description="Individual manager positions with highest portfolio concentration percentages",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_value": ColumnSpec("total_value", "Position value", "float", (0, None)),
        "portfolio_pct": ColumnSpec("portfolio_pct", "Portfolio percentage", "float", (0, 100), nullable=False),
        "risk_level": ColumnSpec(
            "risk_level", "Concentration risk level", "str",
            allowed_values=["Extreme", "Very High", "High", "Moderate", "Low"]
        ),
    }
)

CONTRACT_MOMENTUM_STOCKS = FileContract(
    file_name="momentum_stocks.csv",
    mode="current",
    description="Stocks showing momentum based on coordinated manager activity",
    allowed_horizons="recent-only",
    required_metadata=[],  # This simplified file does not include standard metadata
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "score": ColumnSpec("score", "Momentum score", "float"),
    }
)

CONTRACT_MOST_SOLD_STOCKS = FileContract(
    file_name="most_sold_stocks.csv",
    mode="current",
    description="Stocks with the highest selling activity across managers",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "unique_sellers": ColumnSpec("unique_sellers", "Number of unique sellers", "int", (0, None)),
        "selling_managers": ColumnSpec("selling_managers", "Names of selling managers", "str", is_preview=True),
        "periods": ColumnSpec("periods", "Quarters with selling activity", "str"),
        "shares_sold": ColumnSpec("shares_sold", "Total shares sold", "int", (0, None)),
        "remaining_holders": ColumnSpec("remaining_holders", "Number of remaining holders", "float", (0, None)),
        "exit_rate_pct": ColumnSpec("exit_rate_pct", "Percentage of holders exiting", "float", (0, 100)),
        "total_sells": ColumnSpec("total_sells", "Total number of sell actions", "int", (0, None)),
        "exit_status": ColumnSpec(
            "exit_status", "Selling intensity classification", "str",
            # The four labels momentum_analyzer assigns; the previous list
            # carried three the analyzer never emits and omitted two it does.
            allowed_values=["Partial Exit", "Complete Exit", "Light Selling", "Heavy Exit"]
        ),
        "buy_count": ColumnSpec("buy_count", "Counter-buys during period", "int"),
        "add_count": ColumnSpec("add_count", "Counter-adds during period", "int"),
        "reduce_count": ColumnSpec("reduce_count", "Reduce actions", "int"),
        "sell_count": ColumnSpec("sell_count", "Full sell (exit) actions", "int"),
    }
)

CONTRACT_NEW_POSITIONS = FileContract(
    file_name="new_positions.csv",
    mode="current",
    description="Newly established positions by managers",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "action": ColumnSpec("action", "Action type (Buy)", "str", allowed_values=["Buy"]),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "manager": ColumnSpec("manager", "Manager who initiated position", "str", nullable=False),
        "total_value": ColumnSpec("total_value", "Position value", "float", (0, None)),
        "portfolio_pct": ColumnSpec("portfolio_pct", "Portfolio percentage", "float", (0, 100)),
        "period": ColumnSpec("period", "Quarter when position was established", "str"),
        "shares": ColumnSpec("shares", "Number of shares bought", "int", (0, None)),
    }
)

# Stock price threshold files (5 files with similar structure)
def _create_stocks_under_contract(threshold: int, price_category: str) -> FileContract:
    """Factory function for stocks_under_$X contracts."""
    return FileContract(
        file_name=f"stocks_under_${threshold}.csv",
        mode="current",
        description=f"Stocks trading under ${threshold} with manager interest",
        allowed_horizons="recent-only",
        required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
        forbidden_states=["ERROR", "INVALID"],
        column_specs={
            "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
            "company_name": ColumnSpec("company_name", "Company name", "str"),
            "current_price": ColumnSpec("current_price", "Current market price", "float", (0, threshold)),
            "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
            "managers": ColumnSpec("managers", "Names of managers", "str", is_preview=True),
            "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
            "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
            "index": ColumnSpec("index", "Ranking index", "int", (0, None)),
            "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
            "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
            "recent_buys": ColumnSpec("recent_buys", "Number of recent buy actions", "float", (0, None)),
            "latest_buy_quarter": ColumnSpec("latest_buy_quarter", "Most recent buy quarter", "str"),
            "price_opportunity_score": ColumnSpec("price_opportunity_score", "Price opportunity score", "float", (0, None)),
            "price_category": ColumnSpec(
                "price_category", "Price classification", "str",
                allowed_values=["Ultra-Low Price", "Very Low Price", "Low Price", "Moderate Price", "Value Price", "Affordable", "Mid-Price", "Higher Price"]
            ),
        }
    )

CONTRACT_STOCKS_UNDER_5 = _create_stocks_under_contract(5, "Ultra-Low Price")
CONTRACT_STOCKS_UNDER_10 = _create_stocks_under_contract(10, "Low Price")
CONTRACT_STOCKS_UNDER_20 = _create_stocks_under_contract(20, "Value Price")
CONTRACT_STOCKS_UNDER_50 = _create_stocks_under_contract(50, "Moderate Price")
CONTRACT_STOCKS_UNDER_100 = _create_stocks_under_contract(100, "Standard Price")

CONTRACT_UNDER_RADAR_PICKS = FileContract(
    file_name="under_radar_picks.csv",
    mode="current",
    description="Stocks held by few high-quality managers, potentially undiscovered opportunities",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
        "managers": ColumnSpec("managers", "Names of managers", "str", is_preview=True),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
        "manager_quality": ColumnSpec("manager_quality", "Quality score of holding managers", "float", (0, None)),
        "under_radar_score": ColumnSpec("under_radar_score", "Under-radar opportunity score", "float", (0, None)),
        "first_established": ColumnSpec("first_established", "When first position was established", "str"),
        "recent_additions": ColumnSpec("recent_additions", "Number of recent additions", "float", (0, None)),
        "pick_type": ColumnSpec(
            "pick_type", "Classification of under-radar opportunity", "str",
            allowed_values=["Premium Pick", "Growing Interest", "Hidden Gem", "Early Stage"]
        ),
    }
)

CONTRACT_VALUE_PRICE_OPPORTUNITIES = FileContract(
    file_name="value_price_opportunities.csv",
    mode="current",
    description="Stocks with favorable price characteristics relative to manager activity",
    allowed_horizons="recent-only",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
        "managers": ColumnSpec("managers", "Names of managers", "str", is_preview=True),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "avg_reported_price": ColumnSpec("avg_reported_price", "Average reported purchase price", "float", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "price_change_pct": ColumnSpec("price_change_pct", "Percentage change from reported price", "float"),
        "52_week_low": ColumnSpec("52_week_low", "52-week low price", "float", (0, None)),
        "discount_to_52w_low_pct": ColumnSpec("discount_to_52w_low_pct", "Discount relative to 52-week low", "float"),
        "recent_buy_count": ColumnSpec("recent_buy_count", "Number of recent buy actions", "float", (0, None)),
        "value_opportunity_score": ColumnSpec("value_opportunity_score", "Overall value opportunity score", "float", (0, None)),
        "value_type": ColumnSpec(
            "value_type", "Classification of value opportunity", "str",
            allowed_values=["Active Accumulation", "Value Opportunity", "Near 52W Low", "Price Discount", "Fair Value"]
        ),
    }
)

CONTRACT_STOCK_TIMELINES = FileContract(
    file_name="stock_timelines.csv",
    mode="current",
    description="Timeline summary of manager activity for each stock",
    allowed_horizons="recent-only",
    required_metadata=[],  # This file may not have standard metadata
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "top_managers": ColumnSpec("top_managers", "Top managers by position size", "str", is_preview=True),
        "manager_count": ColumnSpec("manager_count", "Number of managers holding", "int", (0, None)),
        "trend": ColumnSpec(
            "trend", "Overall activity trend", "str",
            allowed_values=["Accumulating", "Slight Accumulate", "Stable", "Slight Distribute", "Distributing", "Neutral"]
        ),
        "momentum": ColumnSpec("momentum", "Momentum score", "float"),
        "buys": ColumnSpec("buys", "Total buy actions", "int", (0, None)),
        "adds": ColumnSpec("adds", "Total add actions", "int", (0, None)),
        "reduces": ColumnSpec("reduces", "Total reduce actions", "int", (0, None)),
        "sells": ColumnSpec("sells", "Total sell actions", "int", (0, None)),
        "recent_activity": ColumnSpec("recent_activity", "Recent activity count", "int", (0, None)),
    }
)

CONTRACT_HIDDEN_GEMS = FileContract(
    file_name="hidden_gems.csv",
    mode="current",
    description="Stocks identified as hidden gems based on multiple criteria",
    allowed_horizons="recent-only",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
    }
)


# =============================================================================
# ADVANCED MODE FILE CONTRACTS (18 files)
# =============================================================================

CONTRACT_ACCUMULATION_VS_DISTRIBUTION = FileContract(
    file_name="accumulation_vs_distribution.csv",
    mode="advanced",
    description="Analysis of accumulation vs distribution phases for each stock",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "unique_managers": ColumnSpec("unique_managers", "Number of unique managers", "int", (0, None)),
        "quarters_active": ColumnSpec("quarters_active", "Number of quarters with activity", "int", (0, None)),
        "buy_add_actions": ColumnSpec("buy_add_actions", "Total buy/add actions", "int", (0, None)),
        "sell_reduce_actions": ColumnSpec("sell_reduce_actions", "Total sell/reduce actions", "int", (0, None)),
        "net_activity": ColumnSpec("net_activity", "Net activity (buy-sell)", "int"),
        "phase": ColumnSpec(
            "phase", "Current phase classification", "str",
            allowed_values=["Accumulating", "Distributing", "Transitioning", "Stable"]
        ),
        "current_shares": ColumnSpec("current_shares", "Current total shares held", "float", (0, None)),
        "current_holders": ColumnSpec("current_holders", "Current number of holders", "float", (0, None)),
        "buy_count": ColumnSpec("buy_count", "New buy actions", "int", (0, None)),
        "add_count": ColumnSpec("add_count", "Add actions", "int", (0, None)),
        "reduce_count": ColumnSpec("reduce_count", "Reduce actions", "int", (0, None)),
        "sell_count": ColumnSpec("sell_count", "Full sell actions", "int", (0, None)),
    }
)

CONTRACT_ACTION_SEQUENCE_PATTERNS = FileContract(
    file_name="action_sequence_patterns.csv",
    mode="advanced",
    description="Analysis of common action sequences and their predictive patterns",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "sequence_pattern": ColumnSpec("sequence_pattern", "Pattern of actions (e.g., 'Buy -> Add -> Add')", "str", nullable=False),
        "total_occurrences": ColumnSpec("total_occurrences", "Number of times pattern occurred", "int", (0, None)),
        "most_likely_next_action": ColumnSpec("most_likely_next_action", "Predicted next action", "str"),
        "predictive_strength": ColumnSpec("predictive_strength", "Confidence in prediction (0-100)", "float", (0, 100)),
        "unique_tickers": ColumnSpec("unique_tickers", "Number of unique stocks with this pattern", "int", (0, None)),
        "unique_managers": ColumnSpec("unique_managers", "Number of unique managers exhibiting pattern", "int", (0, None)),
        "pattern_score": ColumnSpec("pattern_score", "Overall pattern significance score", "float", (0, None)),
        "buy_count": ColumnSpec("buy_count", "Buy actions in pattern", "int", (0, None)),
        "add_count": ColumnSpec("add_count", "Add actions in pattern", "int", (0, None)),
        "reduce_count": ColumnSpec("reduce_count", "Reduce actions in pattern", "int", (0, None)),
        "sell_count": ColumnSpec("sell_count", "Sell actions in pattern", "int", (0, None)),
    }
)

CONTRACT_CATALYST_TIMING_MASTERS = FileContract(
    file_name="catalyst_timing_masters.csv",
    mode="advanced",
    description="Managers with exceptional timing around market catalysts",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name/ID", "str", nullable=False),
        "total_trades": ColumnSpec("total_trades", "Total number of trades analyzed", "int", (0, None)),
        "entry_trades": ColumnSpec("entry_trades", "Number of entry trades", "int", (0, None)),
        "exit_trades": ColumnSpec("exit_trades", "Number of exit trades", "int", (0, None)),
        "perfect_entries": ColumnSpec("perfect_entries", "Number of well-timed entries", "int", (0, None)),
        "perfect_exits": ColumnSpec("perfect_exits", "Number of well-timed exits", "int", (0, None)),
        "entry_success_rate": ColumnSpec("entry_success_rate", "Entry timing success rate (%)", "float", (0, 100)),
        "exit_success_rate": ColumnSpec("exit_success_rate", "Exit timing success rate (%)", "float", (0, 100)),
        "timing_score": ColumnSpec("timing_score", "Overall timing score", "float", (0, 100)),
        "years_active": ColumnSpec("years_active", "Number of years with data", "int", (0, None)),
    }
)

CONTRACT_CRISIS_ALPHA_GENERATORS = FileContract(
    file_name="crisis_alpha_generators.csv",
    mode="advanced",
    description="Managers who generate alpha during market crises",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_crisis_activities": ColumnSpec("total_crisis_activities", "Total actions during crises", "int", (0, None)),
        "buy_during_crisis": ColumnSpec("buy_during_crisis", "Buys during crisis periods", "int", (0, None)),
        "crisis_periods_active": ColumnSpec("crisis_periods_active", "Number of crises participated in", "int", (0, None)),
        "gfc_2008_buy_actions": ColumnSpec("gfc_2008_buy_actions", "Buy actions during 2008 crisis", "int", (0, None)),
        "gfc_2008_total_actions": ColumnSpec("gfc_2008_total_actions", "Total actions during 2008 crisis", "int", (0, None)),
        "gfc_2008_unique_stocks": ColumnSpec("gfc_2008_unique_stocks", "Unique stocks traded in 2008", "int", (0, None)),
        "gfc_2008_buy_ratio_pct": ColumnSpec("gfc_2008_buy_ratio_pct", "Buy ratio during 2008 (%)", "float", (0, 100)),
        "covid_2020_buy_actions": ColumnSpec("covid_2020_buy_actions", "Buy actions during COVID", "int", (0, None)),
        "covid_2020_total_actions": ColumnSpec("covid_2020_total_actions", "Total actions during COVID", "int", (0, None)),
        "covid_2020_unique_stocks": ColumnSpec("covid_2020_unique_stocks", "Unique stocks during COVID", "int", (0, None)),
        "covid_2020_buy_ratio_pct": ColumnSpec("covid_2020_buy_ratio_pct", "Buy ratio during COVID (%)", "float", (0, 100)),
        "inflation_2022_buy_actions": ColumnSpec("inflation_2022_buy_actions", "Buy actions during 2022", "int", (0, None)),
        "inflation_2022_total_actions": ColumnSpec("inflation_2022_total_actions", "Total actions during 2022", "int", (0, None)),
        "inflation_2022_unique_stocks": ColumnSpec("inflation_2022_unique_stocks", "Unique stocks in 2022", "int", (0, None)),
        "inflation_2022_buy_ratio_pct": ColumnSpec("inflation_2022_buy_ratio_pct", "Buy ratio during 2022 (%)", "float", (0, 100)),
        "best_crisis_name": ColumnSpec("best_crisis_name", "Crisis with best performance", "str"),
        "best_crisis_buy_ratio_pct": ColumnSpec("best_crisis_buy_ratio_pct", "Best crisis buy ratio (%)", "float", (0, 100)),
        "best_crisis_buy_actions": ColumnSpec("best_crisis_buy_actions", "Best crisis buy count", "int", (0, None)),
        "best_crisis_total_actions": ColumnSpec("best_crisis_total_actions", "Best crisis total actions", "int", (0, None)),
        "crisis_alpha_score": ColumnSpec("crisis_alpha_score", "Overall crisis alpha score", "float", (0, None)),
        "current_portfolio_value": ColumnSpec("current_portfolio_value", "Current portfolio value", "float", (0, None)),
    }
)

CONTRACT_HIGH_CONVICTION_STOCKS = FileContract(
    file_name="high_conviction_stocks.csv",
    mode="advanced",
    description="Stocks with highest manager conviction based on portfolio concentration",
    allowed_horizons="mixed-explicit",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "top_managers": ColumnSpec("top_managers", "Top managers by conviction", "str", is_preview=True),
        "managers_shown": ColumnSpec("managers_shown", "Number of managers displayed", "int", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Total number of managers holding", "int", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "conviction_score": ColumnSpec("conviction_score", "Overall conviction score", "float", (0, None)),
    }
)

CONTRACT_INTERESTING_STOCKS_OVERVIEW = FileContract(
    file_name="interesting_stocks_overview.csv",
    mode="advanced",
    description="Overview of stocks with interesting characteristics across multiple dimensions",
    allowed_horizons="mixed-explicit",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "top_managers": ColumnSpec("top_managers", "Top managers holding", "str", is_preview=True),
        "managers_shown": ColumnSpec("managers_shown", "Number of managers displayed", "int", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Total number of managers", "int", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "first_reported_date": ColumnSpec("first_reported_date", "Date first reported", "str"),
        "latest_period": ColumnSpec("latest_period", "Most recent period", "str"),
        "buy_count": ColumnSpec("buy_count", "Historical buy count", "float", (0, None)),
        "last_buy_period": ColumnSpec("last_buy_period", "Last buy period", "str"),
        "historical_buyers": ColumnSpec("historical_buyers", "Number of historical buyers", "float", (0, None)),
        "active_managers": ColumnSpec("active_managers", "Currently active managers", "int", (0, None)),
        "appeal_score": ColumnSpec("appeal_score", "Overall appeal score", "float", (0, None)),
        "investment_timing": ColumnSpec(
            "investment_timing", "Timing classification", "str",
            allowed_values=["Strong", "Moderate", "Weak", "Historical"]
        ),
        "reported_price": ColumnSpec("reported_price", "Average reported price", "float", (0, None)),
    }
)

CONTRACT_LONG_TERM_WINNERS = FileContract(
    file_name="long_term_winners.csv",
    mode="advanced",
    description="Stocks with long-term positive manager activity and holding periods",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "first_buy_period": ColumnSpec("first_buy_period", "First buy period", "str"),
        "first_observed_year": ColumnSpec("first_observed_year", "Year first observed", "int", (1990, 2030)),
        "years_held": ColumnSpec("years_held", "Years held by managers", "int", (0, None)),
        "total_historical_actions": ColumnSpec("total_historical_actions", "Total historical actions", "int", (0, None)),
        "net_accumulation": ColumnSpec("net_accumulation", "Net accumulation count", "int"),
        "current_holders": ColumnSpec("current_holders", "Current number of holders", "int", (0, None)),
        "unique_managers_all_time": ColumnSpec("unique_managers_all_time", "Unique managers ever", "int", (0, None)),
        "winner_score": ColumnSpec("winner_score", "Long-term winner score", "float", (0, None)),
    }
)

CONTRACT_MANAGER_EVOLUTION_PATTERNS = FileContract(
    file_name="manager_evolution_patterns.csv",
    mode="advanced",
    description="Analysis of how manager investment styles have evolved over time",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "career_length_years": ColumnSpec("career_length_years", "Years of tracked activity", "int", (0, None)),
        "early_stocks": ColumnSpec("early_stocks", "Unique stocks in early career", "int", (0, None)),
        "late_stocks": ColumnSpec("late_stocks", "Unique stocks in recent period", "int", (0, None)),
        "diversification_change": ColumnSpec("diversification_change", "Change in diversification", "int"),
        "activity_per_year_change": ColumnSpec("activity_per_year_change", "Change in annual activity", "float"),
        "style_change_score": ColumnSpec("style_change_score", "Style evolution score", "float", (0, None)),
        "early_buy_pct": ColumnSpec("early_buy_pct", "Early career buy percentage", "float", (0, 100)),
        "late_buy_pct": ColumnSpec("late_buy_pct", "Recent buy percentage", "float", (0, 100)),
        "total_activities": ColumnSpec("total_activities", "Total activities tracked", "int", (0, None)),
        "evolution_type": ColumnSpec(
            "evolution_type", "Type of evolution pattern", "str",
            # The six labels advanced_analyzer assigns.
            allowed_values=["Stable", "Diversifying", "Concentrating", "Style Shifter", "More Active", "Less Active"]
        ),
        "evolution_score": ColumnSpec("evolution_score", "Overall evolution score", "float", (0, None)),
    }
)

CONTRACT_MANAGER_PERFORMANCE = FileContract(
    file_name="manager_performance.csv",
    mode="advanced",
    description="Performance metrics for each tracked manager",
    allowed_horizons="mixed-explicit",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_value": ColumnSpec("total_value", "Total portfolio value", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average position size", "float", (0, 100)),
        "position_count": ColumnSpec("position_count", "Number of positions", "int", (0, None)),
        "avg_position_value": ColumnSpec("avg_position_value", "Average position value", "float", (0, None)),
        "position_value_std": ColumnSpec("position_value_std", "Position value std deviation", "float", (0, None)),
        "concentration_ratio": ColumnSpec("concentration_ratio", "Portfolio concentration ratio", "float", (0, None)),
        "top_holding": ColumnSpec("top_holding", "Largest holding ticker", "str"),
        "top_holding_value": ColumnSpec("top_holding_value", "Largest holding value", "float", (0, None)),
        "top_holding_pct": ColumnSpec("top_holding_pct", "Largest holding percentage", "float", (0, 100)),
        "diversification_score": ColumnSpec("diversification_score", "Diversification score", "float", (0, 100)),
        "first_year": ColumnSpec("first_year", "First year tracked", "float"),
        "years_active": ColumnSpec("years_active", "Years of activity", "float", (0, None)),
        # total_return_pct / annualized_return_pct were removed: they were
        # synthetic values derived from an assumed 10%/yr growth rate, not
        # measurable from 13F data.
    }
)

CONTRACT_MANAGER_TRACK_RECORDS = FileContract(
    file_name="manager_track_records.csv",
    mode="advanced",
    description="Historical track records for each manager",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "years_active": ColumnSpec("years_active", "Years of tracked activity", "int", (0, None)),
        "first_year": ColumnSpec("first_year", "First year tracked", "int", (1990, 2030)),
        "last_year": ColumnSpec("last_year", "Last year tracked", "int", (1990, 2030)),
        "total_actions": ColumnSpec("total_actions", "Total number of actions", "int", (0, None)),
        "current_holdings": ColumnSpec("current_holdings", "Current number of holdings", "int", (0, None)),
        "buy_actions": ColumnSpec("buy_actions", "Total buy actions", "int", (0, None)),
        "sell_actions": ColumnSpec("sell_actions", "Complete exits (Sell only; Reduce counted separately)", "int", (0, None)),
        "add_actions": ColumnSpec("add_actions", "Total add actions", "int", (0, None)),
        "reduce_actions": ColumnSpec("reduce_actions", "Position decreases (Reduce only)", "int", (0, None)),
        "consistency_score": ColumnSpec("consistency_score", "Consistency score (-1 to 1)", "float", (-1, 1)),
        "2008_financial_actions": ColumnSpec("2008_financial_actions", "Actions during 2008", "float"),
        "2008_financial_buy_ratio": ColumnSpec("2008_financial_buy_ratio", "Buy ratio during 2008", "float"),
        "2020_covid_actions": ColumnSpec("2020_covid_actions", "Actions during COVID", "float"),
        "2020_covid_buy_ratio": ColumnSpec("2020_covid_buy_ratio", "Buy ratio during COVID", "float"),
        "2022_inflation_actions": ColumnSpec("2022_inflation_actions", "Actions during 2022", "float"),
        "2022_inflation_buy_ratio": ColumnSpec("2022_inflation_buy_ratio", "Buy ratio during 2022", "float"),
        "total_crisis_actions": ColumnSpec("total_crisis_actions", "Total crisis actions", "int", (0, None)),
        "crisis_buying_ratio": ColumnSpec("crisis_buying_ratio", "Crisis buying ratio", "float", (0, 1)),
        "current_portfolio_value": ColumnSpec("current_portfolio_value", "Current portfolio value", "float", (0, None)),
        # estimated_initial_value / total_return_pct / annualized_return_pct
        # were removed: fabricated from an assumed 10%/yr growth rate.
        "track_record_score": ColumnSpec("track_record_score", "Overall track record score", "float", (0, None)),
    }
)

CONTRACT_MULTI_MANAGER_FAVORITES = FileContract(
    file_name="multi_manager_favorites.csv",
    mode="advanced",
    description="Stocks favored by multiple managers simultaneously",
    allowed_horizons="mixed-explicit",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "top_managers": ColumnSpec("top_managers", "Top managers holding", "str", is_preview=True),
        "managers_shown": ColumnSpec("managers_shown", "Number of managers displayed", "int", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Total number of managers", "int", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "consensus_score": ColumnSpec("consensus_score", "Multi-manager consensus score", "float", (0, None)),
        "first_period": ColumnSpec("first_period", "First period held", "str"),
        "latest_period": ColumnSpec("latest_period", "Most recent period", "str"),
        "recent_buyers": ColumnSpec("recent_buyers", "Recent buyer count", "float", (0, None)),
    }
)

CONTRACT_POSITION_BUILDING_TIMELINE = FileContract(
    file_name="position_building_timeline.csv",
    mode="advanced",
    description="Detailed timeline of position building for each manager-stock combination",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "action": ColumnSpec("action", "Action taken", "str"),
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "period": ColumnSpec("period", "Quarter of action", "str", nullable=False),
        "action_type": ColumnSpec(
            "action_type", "Type of action", "str",
            allowed_values=["Buy", "Sell", "Add", "Reduce"]
        ),
        "shares_changed": ColumnSpec("shares_changed", "Absolute shares changed", "int"),
        "shares_delta": ColumnSpec("shares_delta", "Signed shares change", "int"),
        "has_complete_history": ColumnSpec("has_complete_history", "Whether history is complete", "bool"),
        "cumulative_shares": ColumnSpec("cumulative_shares", "Running total shares", "int", (0, None)),
        "phase": ColumnSpec(
            "phase", "Position phase", "str",
            allowed_values=["Initial", "Building", "Transitioning", "Reducing", "Exited", "Stable", "Distributing", "Accumulating"]
        ),
    }
)

CONTRACT_POSITION_FLIP_POINTS = FileContract(
    file_name="position_flip_points.csv",
    mode="advanced",
    description="Points where positions flipped from accumulation to distribution or vice versa",
    allowed_horizons="multi-year",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_value": ColumnSpec("total_value", "Current position value", "float", (0, None)),
        "flip_quarter": ColumnSpec("flip_quarter", "Quarter of flip", "str"),
        "flip_action": ColumnSpec("flip_action", "Action that caused flip", "str"),
        "shares_at_flip": ColumnSpec("shares_at_flip", "Shares at flip point", "int", (0, None)),
        "action_sequence": ColumnSpec("action_sequence", "Sequence leading to flip", "str"),
        "quarters_before_flip": ColumnSpec("quarters_before_flip", "Quarters of prior trend", "int", (0, None)),
        "quarters_after_flip": ColumnSpec("quarters_after_flip", "Quarters since flip", "int", (0, None)),
        "traceability_status": ColumnSpec(
            "traceability_status", "Data quality status", "str",
            allowed_values=["matched", "partial", "inferred", "unknown", "delisted", "no_timeline"]
        ),
        "current_shares": ColumnSpec("current_shares", "Current shares held", "float", (0, None)),
        "still_held": ColumnSpec("still_held", "Whether position still exists", "bool"),
    }
)

CONTRACT_POSITION_SIZING_MASTERY = FileContract(
    file_name="position_sizing_mastery.csv",
    mode="advanced",
    description="Analysis of position sizing strategies and efficiency",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "total_positions": ColumnSpec("total_positions", "Number of positions", "int", (0, None)),
        "avg_position_size": ColumnSpec("avg_position_size", "Average position size %", "float", (0, 100)),
        "max_position_size": ColumnSpec("max_position_size", "Maximum position size %", "float", (0, 100)),
        "position_concentration": ColumnSpec("position_concentration", "Position concentration metric", "float", (0, 100)),
        "position_variance": ColumnSpec("position_variance", "Position size variance", "float", (0, None)),
        "small_positions_pct": ColumnSpec("small_positions_pct", "% of small positions", "float", (0, 100)),
        "medium_positions_pct": ColumnSpec("medium_positions_pct", "% of medium positions", "float", (0, 100)),
        "large_positions_pct": ColumnSpec("large_positions_pct", "% of large positions", "float", (0, 100)),
        "sizing_efficiency_score": ColumnSpec("sizing_efficiency_score", "Position sizing efficiency", "float", (0, 100)),
        "total_portfolio_value": ColumnSpec("total_portfolio_value", "Total portfolio value", "float", (0, None)),
        "historical_activities": ColumnSpec("historical_activities", "Total historical activities", "int", (0, None)),
        "sizing_style": ColumnSpec(
            "sizing_style", "Position sizing style", "str",
            allowed_values=["Concentrated", "Balanced", "Diversified", "Barbell", "Systematic", "High Conviction"]
        ),
    }
)

CONTRACT_SECTOR_ROTATION_EXCELLENCE = FileContract(
    file_name="sector_rotation_excellence.csv",
    mode="advanced",
    description="Managers with strong sector rotation strategies",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "manager": ColumnSpec("manager", "Manager name", "str", nullable=False),
        "sectors_traded": ColumnSpec("sectors_traded", "Number of sectors traded", "int", (0, None)),
        "rotation_success_score": ColumnSpec("rotation_success_score", "Sector rotation success score", "float", (0, None)),
        "sectors_list": ColumnSpec("sectors_list", "List of sectors traded", "str"),
    }
)

CONTRACT_SECTOR_ROTATION_PATTERNS = FileContract(
    file_name="sector_rotation_patterns.csv",
    mode="advanced",
    description="Detailed sector rotation patterns aggregated by period and sector",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "period": ColumnSpec("period", "Quarter period (e.g., Q1 2020)", "str", nullable=False),
        "sector": ColumnSpec("sector", "Sector name", "str"),
        "buy_actions": ColumnSpec("buy_actions", "Buy actions in sector", "int", (0, None)),
        "add_actions": ColumnSpec("add_actions", "Add actions in sector", "int", (0, None)),
        "sell_actions": ColumnSpec("sell_actions", "Sell actions in sector", "int", (0, None)),
        "reduce_actions": ColumnSpec("reduce_actions", "Reduce actions in sector", "int", (0, None)),
        "net_flow": ColumnSpec("net_flow", "Net flow (buy+add)-(sell+reduce)", "int"),
        "net_activity": ColumnSpec("net_activity", "Net activity count", "int"),
    }
)

CONTRACT_THEME_EMERGENCE_DETECTION = FileContract(
    file_name="theme_emergence_detection.csv",
    mode="advanced",
    description="Detection of emerging investment themes based on new manager interest",
    allowed_horizons="recent-only",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "total_recent_managers": ColumnSpec("total_recent_managers", "Managers active recently", "int", (0, None)),
        "new_managers_count": ColumnSpec("new_managers_count", "New managers this period", "int", (0, None)),
        "recent_buy_activities": ColumnSpec("recent_buy_activities", "Recent buy activity count", "int", (0, None)),
        "emergence_score": ColumnSpec("emergence_score", "Theme emergence score", "float", (0, None)),
        "new_managers": ColumnSpec("new_managers", "IDs of new managers", "str"),
        "total_managers": ColumnSpec("total_managers", "Total managers ever", "int", (0, None)),
        "new_manager_names": ColumnSpec("new_manager_names", "Names of new managers", "str", is_preview=True),
    }
)

CONTRACT_TOP_HOLDINGS = FileContract(
    file_name="top_holdings.csv",
    mode="advanced",
    description="Top holdings across all tracked managers by total value and manager count",
    allowed_horizons="mixed-explicit",
    required_metadata=["_analysis_type", "_generated", "_periods", "_window_quarters"],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "current_price": ColumnSpec("current_price", "Current market price", "float", (0, None)),
        "top_managers": ColumnSpec("top_managers", "Top managers holding", "str", is_preview=True),
        "managers_shown": ColumnSpec("managers_shown", "Number of managers displayed", "int", (0, None)),
        "manager_count": ColumnSpec("manager_count", "Total number of managers", "int", (0, None)),
        "total_value": ColumnSpec("total_value", "Total value across holders", "float", (0, None)),
        "avg_portfolio_pct": ColumnSpec("avg_portfolio_pct", "Average portfolio percentage", "float", (0, 100)),
        "total_shares": ColumnSpec("total_shares", "Total shares held", "int", (0, None)),
        "max_portfolio_pct": ColumnSpec("max_portfolio_pct", "Maximum portfolio percentage", "float", (0, 100)),
        "portfolio_pct_std": ColumnSpec("portfolio_pct_std", "Portfolio % standard deviation", "float", (0, None)),
    }
)


# =============================================================================
# HISTORICAL MODE FILE CONTRACTS (4 files)
# =============================================================================

CONTRACT_MULTI_DECADE_CONVICTION = FileContract(
    file_name="multi_decade_conviction.csv",
    mode="historical",
    description="Stocks with recorded activity in 10+ distinct calendar years, ranked by conviction score",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "top_managers": ColumnSpec("top_managers", "Top long-term managers", "str", is_preview=True),
        "total_value": ColumnSpec("total_value", "Total current value", "float", (0, None)),
        "years_held": ColumnSpec("years_held", "Distinct calendar years with recorded activity (not continuous tenure)", "int", (0, None)),
        "consistent_managers": ColumnSpec("consistent_managers", "Managers with consistent holdings", "int", (0, None)),
        "total_managers": ColumnSpec("total_managers", "Total managers ever", "int", (0, None)),
        "current_holders": ColumnSpec("current_holders", "Current number of holders", "int", (0, None)),
        "conviction_score": ColumnSpec(
            "conviction_score",
            "Accumulation over distribution: (buy_actions + 0.7*add_actions) / (1 + sell_actions + 0.5*reduce_actions). "
            "A complete exit (Sell) weighs twice a trim (Reduce); the 1 keeps the ratio finite when neither occurred. "
            "Not a return - 13F filings carry no prices.",
            "float", (0, None)
        ),
        "total_activities": ColumnSpec("total_activities", "Total historical activities", "int", (0, None)),
        "buy_actions": ColumnSpec("buy_actions", "New-position actions (Buy only)", "int", (0, None)),
        "add_actions": ColumnSpec("add_actions", "Position increases (Add only)", "int", (0, None)),
        "sell_actions": ColumnSpec("sell_actions", "Complete exits (Sell only; Reduce counted separately)", "int", (0, None)),
        "reduce_actions": ColumnSpec("reduce_actions", "Position decreases (Reduce only)", "int", (0, None)),
        "periods_active": ColumnSpec("periods_active", "Periods with activity", "int", (0, None)),
        "top_manager": ColumnSpec("top_manager", "Manager active in the largest share of the ticker's active years", "str"),
        "top_manager_years": ColumnSpec("top_manager_years", "Distinct years that manager was active in the ticker (not tenure)", "int", (0, None)),
        "top_manager_consistency": ColumnSpec("top_manager_consistency", "Top manager consistency %", "str"),
        "conviction_type": ColumnSpec(
            "conviction_type", "Type of conviction", "str",
            # The three values analyze_multi_decade_conviction can assign.
            # "Long-term Hold" went away with the years_held >= 10 inclusion
            # threshold: every row in this file is now Decade+ or better.
            allowed_values=["Decade+ Conviction", "Multi-Decade Champion", "Consensus Champion"]
        ),
    }
)

CONTRACT_STOCK_LIFE_CYCLES = FileContract(
    file_name="stock_life_cycles.csv",
    mode="historical",
    description="Complete life cycle analysis of stock holdings across all managers",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "ticker": ColumnSpec("ticker", "Stock ticker symbol", "str", nullable=False),
        "company_name": ColumnSpec("company_name", "Company name", "str"),
        "first_year": ColumnSpec("first_year", "First year tracked", "int", (1990, 2030)),
        "last_year": ColumnSpec("last_year", "Last year tracked", "int", (1990, 2030)),
        "years_tracked": ColumnSpec("years_tracked", "Total years tracked (can be negative if data anomaly)", "int"),
        "total_actions": ColumnSpec("total_actions", "Total number of actions", "int", (0, None)),
        "unique_managers": ColumnSpec("unique_managers", "Unique managers ever", "int", (0, None)),
        "currently_held": ColumnSpec("currently_held", "Whether currently held", "bool"),
        "total_buys": ColumnSpec("total_buys", "Total buy actions", "int", (0, None)),
        "total_sells": ColumnSpec("total_sells", "Complete exits (Sell only; Reduce counted separately)", "int", (0, None)),
        "total_adds": ColumnSpec("total_adds", "Total add actions", "int", (0, None)),
        "total_reduces": ColumnSpec("total_reduces", "Position decreases (Reduce only)", "int", (0, None)),
        "first_buy_period": ColumnSpec("first_buy_period", "First buy period", "str"),
        "complete_exit_count": ColumnSpec("complete_exit_count", "Times completely exited", "int", (0, None)),
        "accumulation_score": ColumnSpec(
            "accumulation_score", "Net actions: (Buy + Add) - (Sell + Reduce)", "float"
        ),
        "life_cycle_score": ColumnSpec("life_cycle_score", "Overall life cycle score", "float", (0, None)),
    }
)

CONTRACT_QUARTERLY_ACTIVITY_TIMELINE = FileContract(
    file_name="quarterly_activity_timeline.csv",
    mode="historical",
    description="Timeline of aggregate quarterly activity across all managers",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "period": ColumnSpec("period", "Quarter (e.g., Q1 2020)", "str", nullable=False),
        "total_actions": ColumnSpec("total_actions", "Total actions in period", "int", (0, None)),
        "unique_managers": ColumnSpec("unique_managers", "Active managers", "int", (0, None)),
        "unique_stocks": ColumnSpec("unique_stocks", "Unique stocks traded", "int", (0, None)),
        "buy_actions": ColumnSpec("buy_actions", "Buy actions", "int", (0, None)),
        "sell_actions": ColumnSpec("sell_actions", "Complete exits (Sell only; Reduce counted separately)", "int", (0, None)),
        "add_actions": ColumnSpec("add_actions", "Add actions", "int", (0, None)),
        "reduce_actions": ColumnSpec("reduce_actions", "Position decreases (Reduce only)", "int", (0, None)),
        "net_activity": ColumnSpec("net_activity", "Net activity (buy+add)-(sell+reduce)", "int"),
        "year": ColumnSpec("year", "Year", "int", (2000, 2030)),
    }
)

CONTRACT_CRISIS_RESPONSE_ANALYSIS = FileContract(
    file_name="crisis_response_analysis.csv",
    mode="historical",
    description="Analysis of manager behavior during historical market crises",
    allowed_horizons="multi-year",
    required_metadata=[],
    forbidden_states=["ERROR", "INVALID"],
    column_specs={
        "crisis": ColumnSpec("crisis", "Crisis identifier", "str", nullable=False),
        "period": ColumnSpec("period", "Crisis period(s)", "str"),
        "total_actions": ColumnSpec("total_actions", "Total actions during crisis", "int", (0, None)),
        "buy_actions": ColumnSpec("buy_actions", "Buy actions during crisis", "int", (0, None)),
        "add_actions": ColumnSpec("add_actions", "Add actions during crisis", "int", (0, None)),
        "sell_actions": ColumnSpec("sell_actions", "Complete exits during crisis (Sell only)", "int", (0, None)),
        "reduce_actions": ColumnSpec("reduce_actions", "Position decreases during crisis (Reduce only)", "int", (0, None)),
        "buy_ratio": ColumnSpec("buy_ratio", "Buy ratio (0-1)", "float", (0, 1)),
        "sell_ratio": ColumnSpec("sell_ratio", "Sell-side ratio (Sell+Reduce)/total (0-1)", "float", (0, 1)),
        "top_buyers": ColumnSpec("top_buyers", "Top buyers during crisis", "str", is_preview=True),
        "most_bought": ColumnSpec("most_bought", "Most bought stocks", "str", is_preview=True),
        "most_sold": ColumnSpec("most_sold", "Most sold stocks", "str", is_preview=True),
        "unique_managers": ColumnSpec("unique_managers", "Active managers", "int", (0, None)),
        "unique_stocks": ColumnSpec("unique_stocks", "Unique stocks traded", "int", (0, None)),
    }
)


# =============================================================================
# CONTRACT REGISTRY
# =============================================================================

CONTRACT_REGISTRY: Dict[str, FileContract] = {
    # Current mode files (18 + hidden_gems = 19)
    "52_week_high_sells.csv": CONTRACT_52_WEEK_HIGH_SELLS,
    "52_week_low_buys.csv": CONTRACT_52_WEEK_LOW_BUYS,
    "concentration_changes.csv": CONTRACT_CONCENTRATION_CHANGES,
    "contrarian_opportunities.csv": CONTRACT_CONTRARIAN_OPPORTUNITIES,
    "deep_value_plays.csv": CONTRACT_DEEP_VALUE_PLAYS,
    "high_conviction_low_price.csv": CONTRACT_HIGH_CONVICTION_LOW_PRICE,
    "highest_portfolio_concentration.csv": CONTRACT_HIGHEST_PORTFOLIO_CONCENTRATION,
    "momentum_stocks.csv": CONTRACT_MOMENTUM_STOCKS,
    "most_sold_stocks.csv": CONTRACT_MOST_SOLD_STOCKS,
    "new_positions.csv": CONTRACT_NEW_POSITIONS,
    "stocks_under_$5.csv": CONTRACT_STOCKS_UNDER_5,
    "stocks_under_$10.csv": CONTRACT_STOCKS_UNDER_10,
    "stocks_under_$20.csv": CONTRACT_STOCKS_UNDER_20,
    "stocks_under_$50.csv": CONTRACT_STOCKS_UNDER_50,
    "stocks_under_$100.csv": CONTRACT_STOCKS_UNDER_100,
    "under_radar_picks.csv": CONTRACT_UNDER_RADAR_PICKS,
    "value_price_opportunities.csv": CONTRACT_VALUE_PRICE_OPPORTUNITIES,
    "stock_timelines.csv": CONTRACT_STOCK_TIMELINES,
    "hidden_gems.csv": CONTRACT_HIDDEN_GEMS,
    
    # Advanced mode files (19)
    "accumulation_vs_distribution.csv": CONTRACT_ACCUMULATION_VS_DISTRIBUTION,
    "action_sequence_patterns.csv": CONTRACT_ACTION_SEQUENCE_PATTERNS,
    "catalyst_timing_masters.csv": CONTRACT_CATALYST_TIMING_MASTERS,
    "crisis_alpha_generators.csv": CONTRACT_CRISIS_ALPHA_GENERATORS,
    "high_conviction_stocks.csv": CONTRACT_HIGH_CONVICTION_STOCKS,
    "interesting_stocks_overview.csv": CONTRACT_INTERESTING_STOCKS_OVERVIEW,
    "long_term_winners.csv": CONTRACT_LONG_TERM_WINNERS,
    "manager_evolution_patterns.csv": CONTRACT_MANAGER_EVOLUTION_PATTERNS,
    "manager_performance.csv": CONTRACT_MANAGER_PERFORMANCE,
    "manager_track_records.csv": CONTRACT_MANAGER_TRACK_RECORDS,
    "multi_manager_favorites.csv": CONTRACT_MULTI_MANAGER_FAVORITES,
    "position_building_timeline.csv": CONTRACT_POSITION_BUILDING_TIMELINE,
    "position_flip_points.csv": CONTRACT_POSITION_FLIP_POINTS,
    "position_sizing_mastery.csv": CONTRACT_POSITION_SIZING_MASTERY,
    "sector_rotation_excellence.csv": CONTRACT_SECTOR_ROTATION_EXCELLENCE,
    "sector_rotation_patterns.csv": CONTRACT_SECTOR_ROTATION_PATTERNS,
    "theme_emergence_detection.csv": CONTRACT_THEME_EMERGENCE_DETECTION,
    "top_holdings.csv": CONTRACT_TOP_HOLDINGS,
    
    # Historical mode files (4)
    "multi_decade_conviction.csv": CONTRACT_MULTI_DECADE_CONVICTION,
    "stock_life_cycles.csv": CONTRACT_STOCK_LIFE_CYCLES,
    "quarterly_activity_timeline.csv": CONTRACT_QUARTERLY_ACTIVITY_TIMELINE,
    "crisis_response_analysis.csv": CONTRACT_CRISIS_RESPONSE_ANALYSIS,
}


def get_contract(filename: str) -> Optional[FileContract]:
    """Get the contract for a specific filename.
    
    Args:
        filename: Name of the CSV file (with or without path).
    
    Returns:
        FileContract if found, None otherwise.
    """
    # Extract just the filename if a path was provided
    if "/" in filename or "\\" in filename:
        import os
        filename = os.path.basename(filename)
    
    return CONTRACT_REGISTRY.get(filename)


def get_contracts_by_mode(mode: Literal["current", "historical", "advanced"]) -> Dict[str, FileContract]:
    """Get all contracts for a specific analysis mode.
    
    Args:
        mode: The analysis mode to filter by.
    
    Returns:
        Dictionary of filename -> FileContract for the specified mode.
    """
    return {
        filename: contract
        for filename, contract in CONTRACT_REGISTRY.items()
        if contract.mode == mode
    }


def validate_file(filepath: str, strict: bool = False) -> Tuple[bool, List[Violation]]:
    """Validate a CSV file against its contract.
    
    Args:
        filepath: Path to the CSV file to validate.
        strict: If True, unexpected columns are errors.
    
    Returns:
        Tuple of (is_valid, violations) where is_valid is True if no errors found.
    """
    import os
    
    filename = os.path.basename(filepath)
    contract = get_contract(filename)
    
    if contract is None:
        return False, [Violation(
            violation_type=ViolationType.MISSING_COLUMN,
            column=None,
            message=f"No contract found for file '{filename}'",
            severity="error"
        )]
    
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        return False, [Violation(
            violation_type=ViolationType.INVALID_DTYPE,
            column=None,
            message=f"Failed to read CSV: {str(e)}",
            severity="error"
        )]
    
    violations = validate_against_contract(df, contract, strict)
    
    # Check if there are any error-level violations
    has_errors = any(v.severity == "error" for v in violations)
    
    return not has_errors, violations


def generate_contract_documentation() -> str:
    """Generate markdown documentation for all contracts.
    
    Returns:
        Markdown-formatted documentation string.
    """
    lines = [
        "# File Contract Documentation",
        "",
        "This document describes the expected schema for each CSV output file.",
        "",
    ]
    
    for mode in ["current", "advanced", "historical"]:
        lines.append(f"## {mode.title()} Mode Files")
        lines.append("")
        
        contracts = get_contracts_by_mode(mode)  # type: ignore
        for filename, contract in sorted(contracts.items()):
            lines.append(f"### {filename}")
            lines.append("")
            lines.append(f"**Description:** {contract.description}")
            lines.append("")
            lines.append(f"**Horizon:** {contract.allowed_horizons}")
            lines.append("")
            lines.append("| Column | Type | Description | Nullable |")
            lines.append("|--------|------|-------------|----------|")
            
            for col_name, spec in contract.column_specs.items():
                nullable = "Yes" if spec.nullable else "No"
                lines.append(f"| {col_name} | {spec.dtype} | {spec.meaning} | {nullable} |")
            
            lines.append("")
    
    return "\n".join(lines)


# Module exports
__all__ = [
    "ViolationType",
    "Violation",
    "ColumnSpec",
    "FileContract",
    "validate_against_contract",
    "CONTRACT_REGISTRY",
    "get_contract",
    "get_contracts_by_mode",
    "validate_file",
    "generate_contract_documentation",
    "STANDARD_METADATA_COLUMNS",
]
