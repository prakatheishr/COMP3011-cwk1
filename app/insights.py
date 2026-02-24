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