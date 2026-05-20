"""Pure reaction helpers, free of deltachat2 so they stay unit-testable.

Delta Chat's ReactionsChanged event only signals that a message's reaction set
changed, not what changed, so we diff the current set against the last one we
forwarded to the bridge.
"""

from typing import Any, Dict, List, Set, Tuple


def reactions_by_contact(reactions: Any) -> Dict[int, Set[str]]:
    """Normalize a get_message_reactions result into {contact_id: {emoji}}.

    Accepts both the camelCase JSON-RPC shape (reactionsByContact) and the
    snake_case form, and tolerates a None result.
    """
    if not reactions:
        return {}
    rbc = (
        reactions.get("reactionsByContact")
        or reactions.get("reactions_by_contact")
        or {}
    )
    return {int(cid): set(emojis or []) for cid, emojis in rbc.items()}


def diff_reactions(
    prev: Dict[int, Set[str]], new: Dict[int, Set[str]]
) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    """Return (added, removed) as (contact_id, emoji) pairs between two snapshots."""
    added: List[Tuple[int, str]] = []
    removed: List[Tuple[int, str]] = []
    for cid, emojis in new.items():
        for emoji in emojis - prev.get(cid, set()):
            added.append((cid, emoji))
    for cid, emojis in prev.items():
        for emoji in emojis - new.get(cid, set()):
            removed.append((cid, emoji))
    return added, removed
