import subprocess, sys, os
os.chdir(r"C:\Users\Usuario\Desktop\NachoMarket")
venv = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
result = subprocess.run(
    [venv, "-m", "pytest",
     "tests/test_risk.py",
     "tests/test_blacklist.py",
     "tests/test_market_profitability.py",
     "tests/test_should_exit_by_share.py",
     "tests/test_rolling_drawdown.py",
     "tests/test_category_scorer.py",
     "tests/test_edge_filter.py",
     "tests/test_cash_reserves.py",
     "tests/test_position_limits.py",
     "tests/test_volatility.py",
     "tests/test_safe_compounder.py",
     "tests/test_amm_engine.py",
     "tests/test_orderbook_manager.py",
     "tests/test_trading_director.py",
     "-x", "-q",
     "--ignore=tests/test_ab_tester.py",
     "--ignore=tests/test_allocator.py",
     "--ignore=tests/test_audit_smoke.py",
     "--ignore=tests/test_copy_trade.py",
     "--ignore=tests/test_markets.py",
     "--ignore=tests/test_repositioner.py",
     "--ignore=tests/test_rewards_farmer.py",
     "--ignore=tests/test_reward_tracker.py",
     "--ignore=tests/test_stages.py",
     "--ignore=tests/test_strategy.py",
     "--ignore=tests/test_strategy_monitor.py",
     "--ignore=tests/test_toxic_flow.py",
     "--ignore=tests/test_wall_detector.py",
     "--ignore=tests/test_websocket.py",
     "--ignore=tests/test_websocket_health.py",
     "--ignore=tests/test_regime_detector.py",
     "--ignore=tests/test_performance_metrics.py",
     "--ignore=tests/test_client.py",
     "--ignore=tests/test_reconciliation.py",
    ],
    capture_output=True, text=True, timeout=120
)
print(result.stdout[:5000])
if result.returncode != 0:
    print("STDERR:", result.stderr[-2000:])
sys.exit(result.returncode)
