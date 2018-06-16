"""Run the tests."""
import unittest


def main():
    """Run the tests included in the scripts.

    Note:
        The scripts for tests should end with ``_test.py``.
    """
    suite = unittest.TestLoader().discover('.', pattern='*_test.py')
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()

