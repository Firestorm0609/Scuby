#!/usr/bin/env python3
"""Run all 3 agents"""
from agent import TradingAgent
for name in ["mistral_alpha", "mistral_beta", "mistral_gamma"]:
    agent = TradingAgent(name)
    agent.run_daily()
