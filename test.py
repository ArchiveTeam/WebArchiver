import unittest


def main():
    suite = unittest.TestLoader().discover('.', pattern='*_test.py')
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()

