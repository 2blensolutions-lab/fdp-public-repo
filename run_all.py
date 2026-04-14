"""
run_all.py — Doblen Solutions x Farmacias del Pueblo
Orquestador: corre los 4 scripts en orden y para si alguno falla.

Flujo:
    01_payway_procesar.py  → lee 1_csvs/, genera 2_payway/
    02_zetti_cupones.py    → detecta fecha de 2_payway/, genera 3_cupones/
    03_conciliar.py        → lee 2_payway/ y 3_cupones/, genera 4_conciliacion/
    04_enviar.py           → sube a Drive y manda mail

Correr con: python run_all.py
"""

import subprocess
import sys
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    "01_payway_procesar.py",
    "02_zetti_cupones.py",
    "03_conciliar.py",
    "04_enviar.py",
]


def correr_script(nombre: str) -> bool:
    path = os.path.join(BASE_DIR, nombre)
    print(f"\n{'=' * 60}")
    print(f"  Corriendo: {nombre}")
    print(f"{'=' * 60}")

    resultado = subprocess.run(
        [sys.executable, path],
        cwd=BASE_DIR,
    )

    if resultado.returncode != 0:
        print(f"\n❌ ERROR en {nombre} (código {resultado.returncode}) — abortando.")
        return False

    print(f"\n✓ {nombre} finalizado OK")
    return True


def main():
    inicio = datetime.now()
    print("=" * 60)
    print("  Doblen Solutions x Farmacias del Pueblo")
    print(f"  Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    for script in SCRIPTS:
        ok = correr_script(script)
        if not ok:
            sys.exit(1)

    duracion = (datetime.now() - inicio).seconds
    print(f"\n{'=' * 60}")
    print(f"  ✓ Todo OK — duración: {duracion}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
