"""Launch the Hybrid Ice v2.3+ simulation with graceful fallbacks."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> None:
    script_path = Path(__file__).with_name("Hybrid Ice V2 3 Plus.py")

    print("\n\U0001F680 Running Hybrid Ice v2.3+ Simulation Forge...\n")

    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
    except FileNotFoundError:
        print("⚠️ Unable to locate the Hybrid Ice simulation script.")
        print("Please open 'Hybrid Ice V2 3 Plus.py' in your workspace and run it manually.")
        return
    except subprocess.CalledProcessError as exc:
        print("⚠️ The simulation exited with a non-zero status code.")
        print(f"Exit status: {exc.returncode}")
        return
    except Exception as exc:  # pragma: no cover - defensive catch-all for odd environments
        print("⚠️ Unable to execute directly in this environment.\n")
        print(
            "Please open the 'Hybrid Ice V2 3 Plus' script in your Codex / Canvas workspace "
            "and click ▶️ Run to execute locally."
        )
        print(f"\nError detail: {exc}")
        return

    print("\n✅ Simulation completed successfully.")


if __name__ == "__main__":
    main()
