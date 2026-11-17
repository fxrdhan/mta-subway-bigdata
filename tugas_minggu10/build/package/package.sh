#!/bin/sh
# Assembles the submission package dist/Tugas_Minggu10_Smile/ and dist/Tugas_Minggu10_Smile.zip.
# Usage: sh build/package/package.sh <transformed_dataset.csv> [Dokumentasi_ETL.pdf]
# The CSV is the one the etl-notebook Job writes to /work/output; the PDF defaults to the copy in docs/.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$HERE/../.." && pwd)
CSV=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
PDF=${2:-$W/../docs/TugasMinggu10_BIG-DATA_TK47G06_101032330094-101032300131.pdf}
P=$W/dist/Tugas_Minggu10_Smile

rm -rf "$P" "$P.zip"
mkdir -p "$P/Source_Code" "$P/Dataset" "$P/Documentation" "$P/Screenshots"
cp "$W/ETL_Pipeline.ipynb" "$P/Source_Code/"
# Hanya berkas sumber; cache Python (__pycache__) tidak ikut.
mkdir -p "$P/Source_Code/streaming" "$P/Source_Code/k8s"
cp "$W"/streaming/*.py "$P/Source_Code/streaming/"
cp "$W"/k8s/*.yaml "$P/Source_Code/k8s/"
cp "$HERE/README_Source_Code.md" "$P/Source_Code/README.md"
cp "$CSV" "$P/Dataset/transformed_dataset.csv"
cp "$PDF" "$P/Documentation/Dokumentasi_ETL.pdf"
cp "$W"/screenshots/*.png "$P/Screenshots/"
(cd "$W/dist" && zip -qrX Tugas_Minggu10_Smile.zip Tugas_Minggu10_Smile)
echo "wrote $P.zip"
