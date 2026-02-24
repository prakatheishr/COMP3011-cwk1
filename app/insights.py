from __future__ import annotations


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def render_race_insight(facts: dict, mode: str = "recap", fmt: str = "plain") -> str:
    race = facts.get("race", {})
    podium = facts.get("podium", [])
    dnfs = facts.get("dnfs", [])
    dnf_count = facts.get("dnf_count", 0)
    fastest = facts.get("fastest_lap")

    title = f"{race.get('year')} {race.get('raceName')}"
    circuit = race.get("circuitName")

    if len(podium) >= 3:
        p1, p2, p3 = podium[0], podium[1], podium[2]
        winner = p1.get("driverName")
        second = p2.get("driverName")
        third = p3.get("driverName")
    else:
        winner = second = third = "Unknown"

    # --- recap ---
    if mode == "recap":
        if fmt == "radio":
            base = (
                f"Welcome back to the {title} at {circuit}! "
                f"And it’s {winner} who takes the win, with {second} in second and {third} completing the podium. "
            )
            if dnf_count:
                base += f"It was a dramatic one too — {dnf_count} retirements across the field. "
            if fastest:
                base += f"Fastest lap went to {fastest.get('driverName')} with a {fastest.get('fastestLapTime')}."
            return base.strip()

        # plain
        lines = [
            f"{title} ({circuit})",
            f"Podium: 1) {winner}, 2) {second}, 3) {third}",
            f"DNFs: {dnf_count}",
        ]
        if fastest:
            lines.append(f"Fastest lap: {fastest.get('driverName')} ({fastest.get('fastestLapTime')})")
        return " | ".join(lines)

    # --- impact ---
    # In Phase 5, keep impact light: frame significance without needing season standings yet.
    if mode == "impact":
        if fmt == "radio":
            base = (
                f"Big implications after the {title}! {winner} grabs maximum momentum, "
                f"and that podium shake-up with {second} and {third} could shape the season narrative. "
            )
            if dnf_count:
                base += f"With {dnf_count} cars out, reliability is already becoming a storyline. "
            return base.strip()

        # plain
        return (
            f"Impact: {winner} wins the {title}. "
            f"Podium of {second} and {third} may influence momentum in the season. "
            f"DNF count ({dnf_count}) highlights reliability/incident trends."
        )

    return "Invalid mode"


def render_season_insight(facts: dict, mode: str = "recap", fmt: str = "plain") -> str:
    year = facts.get("year")
    race_count = facts.get("race_count")
    top_drivers = facts.get("top_drivers", [])
    top_constructors = facts.get("top_constructors", [])
    gap = facts.get("champion_points_gap")
    win_share = facts.get("top_constructor_win_share")

    champ = top_drivers[0]["driverName"] if top_drivers else "Unknown"
    champ_pts = top_drivers[0].get("points") if top_drivers else None

    top_team = top_constructors[0]["constructorName"] if top_constructors else "Unknown"

    if mode == "recap":
        if fmt == "radio":
            base = (
                f"And that’s the story of {year}! Over {race_count} races, {champ} comes out on top"
            )
            if champ_pts is not None:
                base += f" with {champ_pts} points"
            base += f", while {top_team} leads the constructors. "
            if gap is not None:
                base += f"The title margin was {gap:.1f} points. "
            if win_share is not None:
                base += f"Top team win share: {win_share*100:.1f}%."
            return base.strip()

        parts = [f"{year} season recap ({race_count} races): champion {champ}"]
        if champ_pts is not None:
            parts[-1] += f" ({champ_pts} pts)"
        parts.append(f"Top constructor: {top_team}")
        if gap is not None:
            parts.append(f"Champion gap: {gap:.1f} pts")
        if win_share is not None:
            parts.append(f"Top constructor win share: {win_share*100:.1f}%")
        return " | ".join(parts)

    if mode == "impact":
        if fmt == "radio":
            base = (
                f"Why {year} mattered: {champ}'s performance set the tone, "
                f"and {top_team}'s strength shaped the competitive balance. "
            )
            if gap is not None:
                base += f"That {gap:.1f}-point gap suggests {'dominance' if gap > 50 else 'a tight battle'}. "
            return base.strip()

        # plain
        msg = f"Impact: {year} was defined by {champ} and {top_team}."
        if gap is not None:
            msg += f" Champion gap: {gap:.1f} points."
        return msg

    return "Invalid mode"

def render_season_comparison_insight(compare_pack: dict, mode: str = "recap", fmt: str = "plain") -> str:
    year = compare_pack.get("year")
    compare_to = compare_pack.get("compare_to")
    deltas = compare_pack.get("deltas", {})
    champ_changed = deltas.get("champion_changed")
    champ_a = deltas.get("champion_year")
    champ_b = deltas.get("champion_compare_to")
    gap_delta = deltas.get("champion_gap_delta")
    win_share_delta = deltas.get("top_constructor_win_share_delta")

    if mode == "recap":
        if fmt == "radio":
            base = f"Comparing {year} to {compare_to}: "
            if champ_changed:
                base += f"we had a new champion — {champ_a} replaces {champ_b}. "
            else:
                base += f"the champion stayed the same ({champ_a}). "
            if gap_delta is not None:
                base += f"The title margin changed by {gap_delta:+.1f} points. "
            if win_share_delta is not None:
                base += f"Top team win share shifted by {win_share_delta*100:+.1f}%. "
            return base.strip()

        parts = [f"Comparison {year} vs {compare_to}: champion {champ_a} vs {champ_b}"]
        parts.append(f"Champion changed: {bool(champ_changed)}")
        if gap_delta is not None:
            parts.append(f"Gap delta: {gap_delta:+.1f} pts")
        if win_share_delta is not None:
            parts.append(f"Win share delta: {win_share_delta*100:+.1f}%")
        return " | ".join(parts)

    if mode == "impact":
        if fmt == "radio":
            base = f"What changed between {compare_to} and {year}? "
            if champ_changed:
                base += f"A shift at the top with {champ_a} taking over. "
            if gap_delta is not None:
                base += "Competition got " + ("tighter" if gap_delta < 0 else "more one-sided") + ". "
            return base.strip()

        msg = f"Impact comparison: {year} vs {compare_to}."
        if champ_changed:
            msg += f" Champion changed to {champ_a}."
        if gap_delta is not None:
            msg += f" Title gap delta: {gap_delta:+.1f} points."
        return msg

    return "Invalid mode"