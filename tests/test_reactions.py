"""Unit tests for the dependency-free reaction helpers."""

import importlib.util
import pathlib
import unittest

# Load reactions.py directly so the test doesn't pull matterdelta/__init__.py,
# which imports deltabot_cli/deltachat2 (native deps not needed for these tests).
_spec = importlib.util.spec_from_file_location(
    "matterdelta_reactions",
    pathlib.Path(__file__).resolve().parent.parent / "matterdelta" / "reactions.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
diff_reactions = _mod.diff_reactions
reactions_by_contact = _mod.reactions_by_contact


class ReactionsByContactTest(unittest.TestCase):
    def test_none(self):
        self.assertEqual(reactions_by_contact(None), {})

    def test_camel_case(self):
        got = reactions_by_contact({"reactionsByContact": {"5": ["👍", "🔥"]}})
        self.assertEqual(got, {5: {"👍", "🔥"}})

    def test_snake_case(self):
        got = reactions_by_contact({"reactions_by_contact": {"7": ["❤️"]}})
        self.assertEqual(got, {7: {"❤️"}})

    def test_empty(self):
        self.assertEqual(reactions_by_contact({"reactionsByContact": {}}), {})


class DiffReactionsTest(unittest.TestCase):
    def test_add_to_empty(self):
        added, removed = diff_reactions({}, {5: {"👍"}})
        self.assertEqual(added, [(5, "👍")])
        self.assertEqual(removed, [])

    def test_remove_to_empty(self):
        added, removed = diff_reactions({5: {"👍"}}, {})
        self.assertEqual(added, [])
        self.assertEqual(removed, [(5, "👍")])

    def test_swap(self):
        added, removed = diff_reactions({5: {"👍"}}, {5: {"❤️"}})
        self.assertEqual(added, [(5, "❤️")])
        self.assertEqual(removed, [(5, "👍")])

    def test_no_change(self):
        added, removed = diff_reactions({5: {"🔥"}}, {5: {"🔥"}})
        self.assertEqual(added, [])
        self.assertEqual(removed, [])

    def test_distinct_contacts(self):
        added, removed = diff_reactions({1: {"👍"}}, {1: {"👍"}, 2: {"👍"}})
        self.assertEqual(added, [(2, "👍")])
        self.assertEqual(removed, [])


if __name__ == "__main__":
    unittest.main()
