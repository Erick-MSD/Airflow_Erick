import logging
from pathlib import Path

# Rutas del pipeline: todo vive dentro de su propia carpeta en dags/
baseDir = Path(__file__).resolve().parent
inputDir = baseDir / "data"  # archivos de entrada
outputDir = baseDir / "output"  # artefactos generados (ignorado en git)

logger = logging.getLogger(__name__)


def extract() -> dict:
    # Lee los archivos de entrada y devuelve lo que necesite el siguiente paso
    logger.info("Leyendo archivos de %s", inputDir)
    return {"inputDir": str(inputDir)}


def transform(extractResult: dict) -> dict:
    # Limpia y transforma los datos extraídos
    logger.info("Transformando datos")
    return extractResult


def load(transformResult: dict) -> None:
    # Guarda o carga el resultado final (archivos, base de datos, etc.)
    outputDir.mkdir(parents=True, exist_ok=True)
    logger.info("Resultado guardado en %s", outputDir)
