#!/usr/bin/env python3
"""Run alpha + gamma agents"""
from agent import TradingAgent
for name in ["mistral_alpha", "mistral_gamma"]:
    agent = TradingAgent(name)
    agent.run_daily()
