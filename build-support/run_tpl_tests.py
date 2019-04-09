#!/usr/bin/env python

import argparse
import os
import subprocess

TARGET_STRING = 'VM main() returned: '

def run(tpl_bin, tpl_file):
  proc = subprocess.run([tpl_bin, tpl_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in reversed(proc.stdout.decode('utf-8').split('\n')):
    idx = line.find(TARGET_STRING)
    if idx != -1:
      return line[idx + len(TARGET_STRING):]
  return None


def check(tpl_bin, tpl_tests_file):
  with open(tpl_tests_file) as tpl_tests:
    folder = os.path.dirname(os.path.abspath(tpl_tests_file))
    num_tests, failed = 0, set()
    print('Tests:')

    for line in tpl_tests:
      tpl_file, expected_output = [x.strip() for x in line.split(',')]
      res = run(tpl_bin, os.path.join(folder, tpl_file))
      num_tests += 1

      report = 'PASS'
      if res is None:
        report = 'ERR'
        failed.add(tpl_file)
      elif res != expected_output:
        report = 'FAIL [expect: {}, actual: {}]'.format(expected_output, res)
        failed.add(tpl_file)

      print('\t{}: {}'.format(tpl_file, report))
    print('{}/{} tests passed.'.format(num_tests - len(failed), num_tests))

    if len(failed) > 0:
      print('{} failed:'.format(len(failed)))
      for fail in failed:
        print('\t{}'.format(fail))
      return -1

    return 0


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-b', dest='tpl_bin', help='TPL Binary.')
  parser.add_argument('-f', dest='tpl_tests_file',
    help='File containing <tpl_test, expected_output> lines.')
  args = parser.parse_args()
  return check(args.tpl_bin, args.tpl_tests_file)


if __name__ == '__main__':
  main()