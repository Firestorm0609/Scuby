#!/usr/bin/env python3
"""Run mistral_beta agent"""
from agent import TradingAgent
agent = TradingAgent("mistral_beta")
agent.run_daily()
