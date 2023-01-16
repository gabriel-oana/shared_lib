import os
import glob
import subprocess
import coverage
import xml.etree.ElementTree as ET


def get_linter_score():
    report_path = "reports/linting.txt"
    with open(report_path, "r") as f:
        lines = f.readlines()

    score = lines[-2].strip().split("at ")[-1].split(' (')[0].replace("/10", "")
    return score


def get_coverage_score():
    class Devnull(object):
        def write(self, *args, **kwargs):
            pass

    cov = coverage.Coverage()
    cov.load()
    total = cov.report(file=Devnull())
    coverage_score = round(total)
    return coverage_score


def get_python_version():
    return ">=3.8"


def get_unittest_results():
    report_path = "reports/unit-tests.xml"
    root = ET.parse(report_path).getroot()

    failures = 0
    skipped = 0
    tests = 0
    for type_tag in root.findall("testsuite"):
        failures = int(type_tag.get("failures"))
        skipped = int(type_tag.get("skipped"))
        tests = int(type_tag.get("tests"))

    text = ""
    if failures > 0:
        text = f"{failures} skipped {tests - failures} passed"

    if skipped > 0:
        text = f"{skipped} skipped {tests - skipped} passed"

    if failures == 0 and skipped == 0:
        text = f"{tests} passed"

    return text


def make_badges():

    red = "#be403c"
    warn = "#c8991d"
    green = "#00a10b"
    img_path = f"{os.path.abspath('')}/img"

    print("Making python badge")
    try:
        python_score = get_python_version()
        if python_score:
            if "python.svg" in os.listdir(img_path):
                os.remove(f'{img_path}/python.svg')

            os.system(f'anybadge -l python -v "{python_score}" -f img/python.svg --color="#0f5fa5"')

    except Exception as e:
        print(str(e))

    print("Making coverage badge")
    try:
        coverage_score = get_coverage_score()
        if coverage_score:
            if "coverage.svg" in os.listdir(img_path):
                os.remove(f'{img_path}/coverage.svg')

            os.system(f'anybadge --value={coverage_score} --file=img/coverage.svg coverage')

    except Exception as e:
        print(str(e))

    print("Making pylint badge")
    try:
        pylint_score = get_coverage_score()
        if pylint_score:
            if "pylint.svg" in os.listdir(img_path):
                os.remove(f'{img_path}/pylint.svg')

            os.system(f'anybadge -l pylint -v "{pylint_score}" -f img/pylint.svg 6=red 7=orange 8=yellow 9=green')

    except Exception as e:
        print(str(e))

    print("Making unittest badge")
    try:
        unittest_score = get_unittest_results()
        if unittest_score:
            if "unittest.svg" in os.listdir(img_path):
                os.remove(f'{img_path}/unittest.svg')

            if 'failed' in unittest_score:
                os.system(f'anybadge -l unittest -v "{unittest_score}" -f img/unittest.svg --color="{red}"')
            elif 'skipped' in unittest_score:
                os.system(f'anybadge -l unittest -v "{unittest_score}" -f img/unittest.svg --color="{warn}"')
            else:
                os.system(f'anybadge -l unittest -v "{unittest_score}" -f img/unittest.svg --color="{green}"')

    except Exception as e:
        print(str(e))

    print("Making release badge")
    try:
        if "release.svg" in os.listdir(img_path):
            os.remove(f'{img_path}/release.svg')

        os.system(f'anybadge -l "released" -v "January 2023" -f img/release.svg --color="#0f5fa5"')

    except Exception as e:
        print(str(e))


if __name__ == '__main__':
    make_badges()
