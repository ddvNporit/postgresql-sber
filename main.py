import argparse
import os
import sys
import unittest

from dotenv import load_dotenv

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-env", "--env", default=".env")
    args, remaining = parser.parse_known_args()
    if os.path.exists(args.env):
        load_dotenv(args.env, override=True)
        print(f"--- Используется конфигурация: {args.env} ---")
    else:
        load_dotenv(override=True)
        print("--- Используется конфигурация: .env (default) ---")
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir='tests', pattern='test_*.py')
    v_level = 2 if '-v' in remaining else 1
    runner = unittest.TextTestRunner(verbosity=v_level)
    result = runner.run(suite)
    sys.exit(not result.wasSuccessful())
