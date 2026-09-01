#!/usr/bin/env python3
"""Run beta + gamma agents"""
from agent import TradingAgent
for name in ["mistral_beta", "mistral_gamma"]:
    agent = TradingAgent(name)
    agent.run_daily()
