#!/usr/bin/env python3
"""Run alpha + beta agents"""
from agent import TradingAgent
for name in ["mistral_alpha", "mistral_beta"]:
    agent = TradingAgent(name)
    agent.run_daily()
