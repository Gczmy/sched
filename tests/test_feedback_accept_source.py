"""Pure source checks for the Linux acceptance worker; no process execution."""
import ast
import unittest

from run_feedback_accept import worker_program


class AcceptanceSourceTests(unittest.TestCase):
    def test_worker_records_one_newline_without_a_literal_backslash(self):
        for content in (None, '{"ok":true}', "not json"):
            with self.subTest(content=content):
                tree = ast.parse(worker_program(content))
                run_write = tree.body[1].value
                self.assertEqual("run\n", ast.literal_eval(run_write.args[0]))

    def test_pathological_regex_input_is_not_embedded_as_a_large_argument(self):
        program = worker_program("ignored", pathological_regex=True)
        self.assertLess(len(program), 256)
        ast.parse(program)


if __name__ == "__main__":
    unittest.main()
