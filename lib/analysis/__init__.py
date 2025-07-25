#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Analysis Package

Investment analysis modules for various strategies and time periods.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

"""
Analysis modules for Dataroma institutional investor data.

This package provides modular analysis capabilities for finding investment
opportunities and hidden gems using institutional investor activity data.
"""

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from .holdings_analyzer import HoldingsAnalyzer, TopHoldingsAnalyzer
from .gems_analyzer import GemsAnalyzer
from .momentum_analyzer import MomentumAnalyzer
from .price_analyzer import PriceAnalyzer, StocksUnderPriceAnalyzer
from .orchestrator import AnalysisOrchestrator

__all__ = [
    # Base classes
    "BaseAnalyzer",
    "MultiAnalyzer",
    
    # Specific analyzers
    "HoldingsAnalyzer", 
    "TopHoldingsAnalyzer",
    "GemsAnalyzer",
    "MomentumAnalyzer", 
    "PriceAnalyzer",
    "StocksUnderPriceAnalyzer",
    
    # Orchestrator
    "AnalysisOrchestrator",
]