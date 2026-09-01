#!/usr/bin/env python3
"""Run mistral_gamma agent"""
from agent import TradingAgent
agent = TradingAgent("mistral_gamma")
agent.run_daily()
