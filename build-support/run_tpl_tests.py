#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys

VM_TARGET_STRING = 'VM main() returned: '
ADAPTIVE_TARGET_STRING = 'ADAPTIVE main() returned: '
JIT_TARGET_STRING = 'JIT main() returned: '
TARGET_STRINGS = [VM_TARGET_STRING, ADAPTIVE_TARGET_STRING, JIT_TARGET_STRING]
ERROR_STRS = ['ERROR', 'error', 'fail', 'abort']

def run(tpl_bin, tpl_file, is_sql):
    args = [tpl_bin]
    if is_sql:
        args.append("-sql")
    args.append(tpl_file)
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = []
    for line in reversed(proc.stdout.decode('utf-8').split('\n')):
        if any(s in line for s in ERROR_STRS):
            return []
        for target_string in TARGET_STRINGS:
            idx = line.find(target_string)
            if idx != -1:
                result.append(line[idx + len(target_string):])
    return result


def check(tpl_bin, tpl_folder, tpl_tests_file):
    with open(tpl_tests_file) as tpl_tests:
        results, failed = dict(), set()
        print('Tests:')

        for line in tpl_tests:
            line = line.strip()
            if not line or line[0] == '#':
                continue
            tpl_file, sql, expected_output = [x.strip() for x in line.split(',')]
            is_sql = sql.lower() == "true"
            res = run(tpl_bin, os.path.join(tpl_folder, tpl_file), is_sql)

            report = 'PASS'
            if not res:
                report = 'ERROR'
                failed.add(tpl_file)
            elif len(res) != 3 or not all(output == expected_output for output in res):
                report = 'FAIL [expect: {}, actual: {}]'.format(expected_output,
                                                                res)
                failed.add(tpl_file)
            results[tpl_file] = report

        # Print all results
        max_test_name = max([len(name) for name in results])
        for name, report in results.items():
            print('\t{:>{pad}}: {}'.format(name, report, pad=max_test_name))

        # Print failed tests
        print('{}/{} tests passed.'.format(len(results) - len(failed), len(results)))

        if len(failed) > 0:
            print('{} failed:'.format(len(failed)))
            for fail in failed:
                print('\t{}'.format(fail))
            sys.exit(-1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='tpl_bin', help='TPL binary.')
    parser.add_argument('-f', dest='tpl_tests_file',
                        help='File containing <tpl_test, expected_output> lines.')
    parser.add_argument('-t', dest='tpl_folder', help='TPL tests folder.')
    args = parser.parse_args()
    check(args.tpl_bin, args.tpl_folder, args.tpl_tests_file)


if __name__ == '__main__':
    main()
