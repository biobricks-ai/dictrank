#!/usr/bin/env python3
"""
Download and build DICTrank FDA cardiotoxicity datasets from Figshare.
Contains Drug-Induced Cardiotoxicity Rank (DICTrank) data with biological features.
Source: https://figshare.com/articles/dataset/DICTrank_data/24312274
DOI: 10.6084/m9.figshare.24312274
License: CC-BY-4.0
"""

from pathlib import Path
import pandas as pd
import requests
import gzip
from io import BytesIO

# Figshare download URLs
DOWNLOADS = {
    'dictrank_binarised': {
        'url': 'https://ndownloader.figshare.com/files/42691171',
        'filename': 'DICTrank_binarised.csv.gz',
        'description': 'FDA Drug-Induced Cardiotoxicity Rank labels'
    },
    'dictrank_ambiguous': {
        'url': 'https://ndownloader.figshare.com/files/42691174',
        'filename': 'DICTrank_unique_ambiguous_compounds.csv',
        'description': 'Compounds with ambiguous cardiotoxicity classification'
    },
    'cellpainting': {
        'url': 'https://ndownloader.figshare.com/files/42691156',
        'filename': 'CellPainting_processed.csv.gz',
        'description': 'Cell Painting morphological features (1783 features)'
    },
    'gene_expression': {
        'url': 'https://ndownloader.figshare.com/files/42691165',
        'filename': 'LINCSL1000_processed.csv.gz',
        'description': 'L1000 Gene Expression features (978 landmark genes)'
    },
    'gene_ontology': {
        'url': 'https://ndownloader.figshare.com/files/42691162',
        'filename': 'GeneOntology_processed.csv.gz',
        'description': 'Gene Ontology annotations (4428 GO terms)'
    },
    'cmax': {
        'url': 'https://ndownloader.figshare.com/files/42691159',
        'filename': 'Cmax_processed.csv.gz',
        'description': 'Maximum plasma concentration data'
    },
    'moa': {
        'url': 'https://ndownloader.figshare.com/files/42691168',
        'filename': 'MOA_processed.csv.gz',
        'description': 'Mechanism of Action annotations'
    },
    'sider': {
        'url': 'https://ndownloader.figshare.com/files/42691153',
        'filename': 'sider_binarised.csv.gz',
        'description': 'SIDER side effects binarized'
    },
}


def download_and_convert(name: str, info: dict, output_dir: Path) -> int:
    """Download a CSV/CSV.GZ file and convert to parquet."""
    url = info['url']
    filename = info['filename']
    print(f"Downloading {name} from {url}...")

    try:
        response = requests.get(url, timeout=600)
        response.raise_for_status()
    except Exception as e:
        print(f"  - Error downloading {name}: {e}")
        return 0

    content = response.content

    # Decompress if gzipped
    if filename.endswith('.gz'):
        try:
            content = gzip.decompress(content)
        except Exception as e:
            print(f"  - Error decompressing {name}: {e}")
            return 0

    # Read as CSV
    try:
        from io import StringIO
        df = pd.read_csv(StringIO(content.decode('utf-8')), low_memory=False)
    except Exception as e:
        print(f"  - Error parsing {name}: {e}")
        return 0

    if len(df) == 0:
        print(f"  - Empty dataframe for {name}")
        return 0

    # Standardize column names
    df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]

    # Convert object columns to string for parquet compatibility
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', '')

    # Save to parquet
    output_file = output_dir / f"dictrank_{name}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"  - Saved {len(df):,} records ({len(df.columns)} cols) to {output_file.name}")

    return len(df)


def main():
    brick_path = Path("brick")
    brick_path.mkdir(exist_ok=True)

    print("=" * 60)
    print("Downloading DICTrank FDA Cardiotoxicity datasets from Figshare...")
    print("DOI: 10.6084/m9.figshare.24312274")
    print("=" * 60)

    total_records = 0

    for name, info in DOWNLOADS.items():
        records = download_and_convert(name, info, brick_path)
        total_records += records

    # Print summary
    print("\n" + "=" * 60)
    print("Output files:")
    print("=" * 60)
    for f in sorted(brick_path.glob("*.parquet")):
        df = pd.read_parquet(f)
        print(f"  - {f.name}: {len(df):,} rows, {len(df.columns)} columns")

    print(f"\nTotal: {total_records:,} records")


if __name__ == "__main__":
    main()
