"""Check examples against independent Python aggregation, with process timeouts."""
from collections import Counter
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

BUILD = Path(sys.argv.pop(1) if len(sys.argv) > 1 else "build").resolve()


class Examples(unittest.TestCase):
    def invoke(self, name, *args, ok=True):
        result = subprocess.run([str(BUILD / name), *map(str, args)],
                                text=True, capture_output=True, timeout=20)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def rows(self, output, convert):
        pairs = [line.split("\t") for line in output.splitlines()]
        result = {key: convert(value) for key, value in pairs}
        self.assertEqual(len(pairs), len(result), "duplicate reducer keys")
        return result

    def test_wordcount(self):
        rng = random.Random(379)
        words = [f"word{i}" for i in range(100)] + ["Apple", "apple", "café"]
        expected = Counter()
        with tempfile.TemporaryDirectory() as tmp:
            files = []
            for i in range(8):
                tokens = [rng.choice(words) for _ in range(350)]
                expected.update(tokens)
                path = Path(tmp) / f"input {i}.txt"
                path.write_text(" \t\n".join(tokens), encoding="utf-8")
                files.append(path)
            empty = Path(tmp) / "empty.txt"
            empty.write_text(" \n\t\r\v\f")
            files.append(empty)
            for workers, partitions in [(1, 1), (8, 1), (4, 32)]:
                with self.subTest(workers=workers, partitions=partitions):
                    result = self.invoke("wordcount", "-j", workers, "-p", partitions, *files)
                    self.assertEqual(self.rows(result.stdout, int), dict(expected))

    def test_sum(self):
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / "a.tsv", Path(tmp) / "b.tsv"
            first.write_text("books\t12.5\nmusic 8\n\nbooks 7.5\n")
            second.write_text("music -2\nbooks 0.25\nzero 0")
            for workers, partitions in [(1, 1), (8, 1), (4, 32)]:
                result = self.invoke("sum_by_key", "-j", workers, "-p", partitions, first, second)
                self.assertEqual(self.rows(result.stdout, float),
                                 {"books": 20.25, "music": 6, "zero": 0})

    def test_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty"
            path.touch()
            for name in ["wordcount", "sum_by_key"]:
                self.assertEqual(self.invoke(name, path).stdout, "")

    def test_bad_input(self):
        for name in ["wordcount", "sum_by_key"]:
            self.invoke(name, ok=False)
            self.invoke(name, "missing-file", ok=False)
            self.invoke(name, "-j", "0", "examples/words.txt", ok=False)
            self.invoke(name, "-p", "-1", "examples/words.txt", ok=False)
            self.assertIn("Usage:", self.invoke(name, "-h").stdout)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "invalid"
            for text in ["key", "key 2 extra", "key NaN", "key 1e999", "key nope"]:
                path.write_text(text)
                self.invoke("sum_by_key", path, ok=False)


if __name__ == "__main__":
    unittest.main()
