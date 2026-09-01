#!/usr/bin/env python3
"""Run mistral_alpha agent"""
from agent import TradingAgent
agent = TradingAgent("mistral_alpha")
agent.run_daily()
